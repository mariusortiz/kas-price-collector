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

# ---- Fetchers ----

def get_bybit():
    # endpoint v5
    url = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=KASUSDT"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        j = r.json()
        it = j["result"]["list"][0]
        return dict(
            ex="bybit", pair=PAIR_DISPLAY,
            last=float(it["lastPrice"]),
            bid=float(it["bid1Price"]),
            ask=float(it["ask1Price"]),
            ts=None,
        )
    except requests.HTTPError as e:
        # fallback v3 si 403/5xx
        if getattr(e.response, "status_code", None) in (403, 429, 500, 503):
            url2 = "https://api.bybit.com/spot/v3/public/quote/ticker/24hr?symbol=KASUSDT"
            r2 = requests.get(url2, headers=HEADERS, timeout=TIMEOUT)
            r2.raise_for_status()
            j2 = r2.json()["result"]
            return dict(
                ex="bybit", pair=PAIR_DISPLAY,
                last=float(j2["lastPrice"]),
                bid=float(j2["bidPrice"]),
                ask=float(j2["askPrice"]),
                ts=None,
            )
        raise

def get_okx():
    url = "https://www.okx.com/api/v5/market/ticker?instId=KAS-USDT"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    data = j.get("data", [])
    if not data:
        raise RuntimeError(f"empty data from OKX: {j}")
    d = data[0]
    return dict(
        ex="okx", pair=PAIR_DISPLAY,
        last=float(d["last"]),
        bid=float(d["bidPx"]),
        ask=float(d["askPx"]),
        ts=int(d["ts"]),
    )

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

FETCHERS = [get_bybit, get_okx, get_kucoin]

def collect_once():
    asof = datetime.now(timezone.utc)
    quotes, errors = [], []

    for fn in FETCHERS:
        try:
            q = fn()
            q["mid"] = (q["bid"] + q["ask"]) / 2.0
            q["ts"] = q["ts"] or int(asof.timestamp() * 1000)  # fallback pour Bybit
            quotes.append(q)
        except Exception as e:
            errors.append(f"{fn.__name__}: {e}")

    med = median([q["mid"] for q in quotes]) if quotes else None
    spread_bps = (
        10000.0 * (max(q["mid"] for q in quotes) - min(q["mid"] for q in quotes)) / med
        if quotes and med else None
    )

    return dict(
        asof_iso=asof.isoformat(timespec="seconds"),
        quotes=quotes,
        median=med,
        spread_max_bps=spread_bps,
        errors=errors,
    )

def append_csv(payload):
    """Append une ligne synthétique dans CSV_PATH."""
    df_map = {q["ex"]: q for q in payload["quotes"]}
    row = {
        "timestamp_iso": payload["asof_iso"],
        "bybit_last": df_map.get("bybit", {}).get("last", ""),
        "okx_last": df_map.get("okx", {}).get("last", ""),
        "kucoin_last": df_map.get("kucoin", {}).get("last", ""),
        "median_mid": round(payload["median"], 10) if payload["median"] else "",
        "spread_max_bps": round(payload["spread_max_bps"], 4) if payload["spread_max_bps"] else "",
        "sources_ok": len(payload["quotes"]),
        "errors": " | ".join(payload["errors"]) if payload["errors"] else "",
    }
    fieldnames = list(row.keys())

    # assure le dossier
    import os
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
