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
save_csv = st.sidebar.checkbox("Enregistrer dans un CSV", value=True)
interval = st.sidebar.slider("Auto-refresh (secondes)", 0, 60, 0, help="0 = désactivé")
st.sidebar.write("CSV :", f"`{CSV_PATH}`")

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
    L'ancien endpoint v2 remonte parfois des valeurs aberrantes.
    On passe à quotation v3 + contrôle des champs.
    """
    r = requests.get(
        "https://api-cloud.bitmart.com/spot/quotation/v3/ticker",
        params={"symbol": "KAS_USDT"},
        headers=HEADERS, timeout=TIMEOUT
    )
    r.raise_for_status()
    j = r.json()
    d = (j.get("data") or {}).get("ticker")
    if not d:
        raise RuntimeError(f"empty data from bitmart: {j}")

    last = float(d["last"])
    bid  = float(d["buy_one"])   # book best bid
    ask  = float(d["sell_one"])  # book best ask

    # garde-fous simples pour éviter le 1.8x fantôme
    if not (0.01 < last < 1.0) or not (0.01 < bid < 1.0) or not (0.01 < ask < 1.0):
        raise RuntimeError(f"bitmart out-of-range values: last={last}, bid={bid}, ask={ask}")

    return dict(ex="bitmart", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=None)




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

    # 2) garde-fou contre une valeur délirante (ex: Bitmart à 1.8…)
    if len(quotes) >= 2:
        mids = [q["mid"] for q in quotes]
        provisional_med = median(mids)
        kept, dropped = [], []
        for q in quotes:
            # on écarte si |écart| > 5% de la médiane provisoire
            if abs(q["mid"] - provisional_med) / provisional_med <= 0.05:
                kept.append(q)
            else:
                dropped.append(q)
        if dropped and kept:
            errors.append("outliers dropped: " + ", ".join(f"{q['ex']}={q['mid']:.6f}" for q in dropped))
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

def append_csv(payload):
    """Append une ligne synthétique dans CSV_PATH."""
    df_map = {q["ex"]: q for q in payload["quotes"]}

    row = {
        "timestamp_iso": payload["asof_iso"],
        "bybit_last":   df_map.get("bybit",   {}).get("last", ""),
        "okx_last":     df_map.get("okx",     {}).get("last", ""),
        "kucoin_last":  df_map.get("kucoin",  {}).get("last", ""),
        "gate_last":    df_map.get("gate",    {}).get("last", ""),
        "mexc_last":    df_map.get("mexc",    {}).get("last", ""),
        "bitmart_last": df_map.get("bitmart", {}).get("last", ""),
        "bitget_last":  df_map.get("bitget",  {}).get("last", ""),
        "median_mid": round(payload["median"], 10) if payload["median"] is not None else "",
        "spread_max_bps": round(payload["spread_max_bps"], 4) if payload["spread_max_bps"] is not None else "",
        "sources_ok": len(payload["quotes"]),
        "errors": " | ".join(payload["errors"]) if payload["errors"] else "",
    }

    fieldnames = list(row.keys())

    import os, csv
    os.makedirs(os.path.dirname(CSV_PATH) or ".", exist_ok=True)
    new_file = not os.path.exists(CSV_PATH)

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            w.writeheader()
        w.writerow(row)


# ---- UI controls ----
col1, col2 = st.columns([1, 1])
run_now = col1.button("▶ Collecter maintenant", type="primary")
auto = col2.checkbox("Auto-refresh (activer)", value=False, help="Utilise l'intervalle choisi dans la sidebar.")

out_placeholder = st.empty()

def render_once():
    payload = collect_once()

    if save_csv:
        append_csv(payload)

    quotes = payload["quotes"]
    df = pd.DataFrame(quotes) if quotes else pd.DataFrame(columns=["ex","pair","last","bid","ask","mid","ts"])

    out_placeholder.subheader("🧾 Quotes")
    out_placeholder.dataframe(
        df[["ex","pair","last","bid","ask","mid","ts"]].sort_values("ex"),
        use_container_width="stretch", hide_index=True
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

# boucle d'auto-refresh
if auto and interval > 0:
    render_once()
    st.caption(f"Auto-refresh actif : toutes les {interval} s. (désactive la case pour arrêter)")
    time.sleep(interval)
    st.rerun()
