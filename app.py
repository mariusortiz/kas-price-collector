import time
import csv
from datetime import datetime, timezone
import requests
import pandas as pd
import streamlit as st
from statistics import median

# ---------- Config ----------
PAIR_DISPLAY = "KAS/USDT"
CSV_PATH = "data/kas_prices.csv"
TIMEOUT = 10
# 👉 ajoute cette ligne :
HEADERS = {"User-Agent": "kas-price-collector/1.0 (+https://for-mining.fr)"}
# -----------------------------

st.set_page_config(page_title="KAS Price Collector", layout="centered")
st.title("KAS Price Oracle — Streamlit")

st.caption("Collecte directe via APIs Bybit / OKX / KuCoin, calcule médiane & écart max (bps).")

# ---- Sidebar ----
st.sidebar.header("Options")
interval = st.sidebar.slider("Auto-refresh (secondes)", 0, 60, 0, help="0 = désactivé")
st.sidebar.write("CSV :", f"`{CSV_PATH}`")

outlier_pct = st.sidebar.slider(
    "Seuil anti-outliers (%)",
    min_value=1, max_value=15, value=5, step=1,
    help="Écarte une source si son mid diffère de plus de X % de la médiane provisoire."
)

st.sidebar.subheader("Sources")

use_kucoin  = st.sidebar.checkbox("KuCoin",  value=True)
use_gate    = st.sidebar.checkbox("Gate.io", value=True)
use_mexc    = st.sidebar.checkbox("MEXC",    value=True)
use_bitget  = st.sidebar.checkbox("Bitget",  value=False)
use_bitmart = st.sidebar.checkbox("BitMart", value=False)
# (Bybit et OKX désactivés pour l'instant)

# ---- Fetchers ----

def get_gate():
    # Doc: GET /api/v4/spot/tickers?currency_pair=KAS_USDT
    url = "https://api.gateio.ws/api/v4/spot/tickers"
    r = requests.get(url, params={"currency_pair": "KAS_USDT"}, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    if not j:
        raise RuntimeError("empty data from gate")
    d = j[0]
    last = float(d["last"])
    bid  = float(d["highest_bid"])
    ask  = float(d["lowest_ask"])
    # pas toujours de ts → on homogénéise plus bas
    return dict(ex="gate", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=None)


def get_mexc():
    # bookTicker → bid/ask
    bt = requests.get(
        "https://api.mexc.com/api/v3/ticker/bookTicker",
        params={"symbol": "KASUSDT"},
        headers=HEADERS, timeout=TIMEOUT
    )
    bt.raise_for_status()
    bb = bt.json()
    bid = float(bb["bidPrice"])
    ask = float(bb["askPrice"])

    # last → endpoint price (1 appel léger)
    pr = requests.get(
        "https://api.mexc.com/api/v3/ticker/price",
        params={"symbol": "KASUSDT"},
        headers=HEADERS, timeout=TIMEOUT
    )
    pr.raise_for_status()
    last = float(pr.json()["price"])

    return dict(ex="mexc", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=None)

def get_kucoin():
    url = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=KAS-USDT"
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    d = r.json()["data"]
    return dict(
        ex="kucoin", pair=PAIR_DISPLAY,
        last=float(d["price"]),
        bid=float(d["bestBid"]),
        ask=float(d["bestAsk"]),
        ts=int(d["time"]),
    )

def get_bitget():
    """
    Bitget sert des champs différents selon l'endpoint/région.
    On tente v1 single -> v1 list et on mappe plusieurs noms possibles.
    """
    params = {"symbol": "KASUSDT"}
    # 1) ticker single
    try:
        r = requests.get("https://api.bitget.com/api/spot/v1/market/ticker",
                         params=params, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        j = r.json()
        d = j.get("data") or {}
    except Exception:
        # 2) tickers list
        r = requests.get("https://api.bitget.com/api/spot/v1/market/tickers",
                         params=params, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        j = r.json()
        arr = j.get("data") or []
        if not arr:
            raise RuntimeError(f"empty data from bitget: {j}")
        d = arr[0]

    def pick(*keys):
        for k in keys:
            if k in d and d[k] not in (None, "", "0", 0):
                return float(d[k])
        raise KeyError(f"missing any of {keys} in bitget payload: {d.keys()}")

    last = pick("close", "last", "lastPr")
    bid  = pick("bestBid", "buyOne", "bidPr")
    ask  = pick("bestAsk", "sellOne", "askPr")
    return dict(ex="bitget", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=None)


def get_bitmart():
    """
    Bitmart renvoie ici 'data' avec des clés 'bid_px', 'ask_px', 'last', 'ts', 'symbol', …
    Exemple reçu :
      {'code': 1000, 'data': {'v_24h': '...', 'bid_px': '0.07509', 'ask_px': '0.07572', 'ts': '...', 'last': '0.07531'}, 'message': 'success'}
    """
    r = requests.get(
        "https://api-cloud.bitmart.com/spot/quotation/v3/ticker",
        params={"symbol": "KAS_USDT"},
        headers=HEADERS, timeout=TIMEOUT
    )
    r.raise_for_status()
    j = r.json()
    d = j.get("data")
    if not isinstance(d, dict):
        raise RuntimeError(f"empty data from bitmart: {j}")

    # champs principaux
    last = float(d["last"])
    # certaines versions exposent 'buy_one'/'sell_one', d'autres 'bid_px'/'ask_px'
    bid = float(d.get("bid_px") or d.get("buy_one"))
    ask = float(d.get("ask_px") or d.get("sell_one"))

    # garde-fou simple pour éviter une valeur délirante
    if not (0.01 < last < 1.0) or not (0.01 < bid < 1.0) or not (0.01 < ask < 1.0):
        raise RuntimeError(f"bitmart out-of-range values: last={last}, bid={bid}, ask={ask}")

    ts = int(d.get("ts") or 0)
    return dict(ex="bitmart", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=ts)


FETCHERS = []
if use_kucoin:  FETCHERS.append(get_kucoin)
if use_gate:    FETCHERS.append(get_gate)
if use_mexc:    FETCHERS.append(get_mexc)
if use_bitget:  FETCHERS.append(get_bitget)
if use_bitmart: FETCHERS.append(get_bitmart)


def collect_once():
    asof = datetime.now(timezone.utc)
    quotes, errors = [], []

    # 1) collecte brute
    for fn in FETCHERS:
        try:
            q = fn()
            q["mid"] = (q["bid"] + q["ask"]) / 2.0
            q["ts"] = q["ts"] or int(asof.timestamp() * 1000)  # fallback pour les sources sans ts
            quotes.append(q)
        except Exception as e:
            errors.append(f"{fn.__name__}: {e}")

    # 2) filtre anti-outliers (paramétrable)
    if len(quotes) >= 2:
        mids = [q["mid"] for q in quotes]
        provisional_med = median(mids)
        threshold = outlier_pct / 100.0  # <-- slider sidebar
        kept, dropped = [], []
        for q in quotes:
            if provisional_med and abs(q["mid"] - provisional_med) / provisional_med <= threshold:
                kept.append(q)
            else:
                dropped.append(q)
        if dropped and kept:
            errors.append(
                "outliers dropped (>{:.1f}%): {}".format(
                    outlier_pct, ", ".join(f"{q['ex']}={q['mid']:.6f}" for q in dropped)
                )
            )
            quotes = kept

    # 3) agrégats finaux
    if quotes:
        med = median([q["mid"] for q in quotes])
        spread_bps = (
            10000.0 * (max(q["mid"] for q in quotes) - min(q["mid"] for q in quotes)) / med
            if len(quotes) >= 2 else 0.0
        )
    else:
        med = None
        spread_bps = None

    return {
        "asof_iso": asof.isoformat(timespec="seconds"),
        "quotes": quotes,
        "median": med,
        "spread_max_bps": spread_bps,
        "errors": errors,
    }


# ---- UI controls ----
col1, col2 = st.columns([1, 1])
run_now = col1.button("▶ Collecter maintenant", type="primary")
auto = col2.checkbox("Auto-refresh (activer)", value=False, help="Utilise l'intervalle choisi dans la sidebar.")

out_placeholder = st.empty()

def csv_bytes_from_quotes(payload):
    """CSV des quotes courantes (une ligne par exchange)."""
    import pandas as pd
    quotes = payload.get("quotes", [])
    if not quotes:
        return b""
    df = pd.DataFrame(quotes)[["ex","pair","last","bid","ask","mid","ts"]]
    return df.to_csv(index=False).encode("utf-8")

def csv_bytes_from_summary(payload):
    """CSV d'une seule ligne 'synthèse' (médiane, spread, derniers prix par source)."""
    row = {"timestamp_iso": payload["asof_iso"],
           "median_mid": payload["median"],
           "spread_max_bps": payload["spread_max_bps"],
           "sources_ok": len(payload["quotes"])}
    # ajoute dynamiquement les last par exchange
    for q in payload["quotes"]:
        row[f"{q['ex']}_last"] = q["last"]
    import pandas as pd
    df = pd.DataFrame([row])
    return df.to_csv(index=False).encode("utf-8")

def render_once():
    payload = collect_once()

    if save_csv:
        append_csv(payload)

    quotes = payload["quotes"]
    df = pd.DataFrame(quotes) if quotes else pd.DataFrame(columns=["ex","pair","last","bid","ask","mid","ts"])

    out_placeholder.subheader("🧾 Quotes")
    out_placeholder.dataframe(
    df[["ex","pair","last","bid","ask","mid","ts"]].sort_values("ex"),
    width="stretch",  # ✅ remplace l'ancien use_container_width
    hide_index=True
)

    c1, c2, c3 = st.columns(3)
    c1.metric("Médiane (mid)", f"{payload['median']:.8f}" if payload["median"] else "—")
    c2.metric("Écart max (bps)", f"{payload['spread_max_bps']:.2f}" if payload["spread_max_bps"] is not None else "—")
    c3.metric("Sources valides", str(len(quotes)))

    st.caption(f"asof (UTC) = {payload['asof_iso']}")

    if payload["errors"]:
        st.warning("Sources en échec : " + " | ".join(payload["errors"]))

if run_now:
    render_once()

# --- boutons de téléchargement CSV ---
ts_safe = payload["asof_iso"].replace(":", "-")
st.download_button(
    "⬇️ Télécharger les quotes (CSV)",
    data=csv_bytes_from_quotes(payload),
    file_name=f"kas_quotes_{ts_safe}.csv",
    mime="text/csv",
    type="secondary",
)
st.download_button(
    "⬇️ Télécharger la ligne synthèse (CSV)",
    data=csv_bytes_from_summary(payload),
    file_name=f"kas_summary_{ts_safe}.csv",
    mime="text/csv",
    type="secondary",
)

# boucle d'auto-refresh
if auto and interval > 0:
    render_once()
    st.caption(f"Auto-refresh actif : toutes les {interval} s. (désactive la case pour arrêter)")
    time.sleep(interval)
    st.rerun()
