import time
from datetime import datetime, timezone
import requests
import pandas as pd
import streamlit as st
from statistics import median

# ---------- Config ----------
PAIR_DISPLAY = "KAS/USDT"
TIMEOUT = 10
HEADERS = {"User-Agent": "kas-price-collector/1.0 (+https://for-mining.fr)"}
# -----------------------------

st.set_page_config(page_title="KAS Price Collector", layout="centered")
st.title("KAS Price Oracle — Streamlit")
st.caption("Collecte directe via APIs Gate / MEXC / KuCoin (+ options), calcule médiane & écart max (bps).")

# ---- Sidebar ----
st.sidebar.header("Options")
interval = st.sidebar.slider("Auto-refresh (secondes)", 0, 60, 0, help="0 = désactivé")

# ---- Options qualité ----
outlier_pct = st.sidebar.slider(
    "Seuil anti-outliers (%)",
    min_value=1, max_value=15, value=5, step=1,
    help="Écarte une source si son mid diffère de plus de X % de la médiane provisoire."
)

# ---- Orderbooks ----
show_ob = st.sidebar.checkbox("Afficher les orderbooks", value=False)
ob_depth = st.sidebar.slider("Profondeur carnet (niveaux)", 5, 50, 10, step=5)

# ---- Sources ----
st.sidebar.subheader("Sources")
use_kucoin  = st.sidebar.checkbox("KuCoin",  value=True)
use_gate    = st.sidebar.checkbox("Gate.io", value=True)
use_mexc    = st.sidebar.checkbox("MEXC",    value=True)
use_bitmart = st.sidebar.checkbox("BitMart", value=False)
use_bitget  = st.sidebar.checkbox("Bitget",  value=False)

# ---- Fetchers ----
def get_gate():
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
    return dict(ex="gate", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=None)

def get_mexc():
    bt = requests.get("https://api.mexc.com/api/v3/ticker/bookTicker",
                      params={"symbol": "KASUSDT"}, headers=HEADERS, timeout=TIMEOUT)
    bt.raise_for_status()
    bb = bt.json()
    bid = float(bb["bidPrice"])
    ask = float(bb["askPrice"])
    pr = requests.get("https://api.mexc.com/api/v3/ticker/price",
                      params={"symbol": "KASUSDT"}, headers=HEADERS, timeout=TIMEOUT)
    pr.raise_for_status()
    last = float(pr.json()["price"])
    return dict(ex="mexc", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=None)

def get_kucoin():
    url = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=KAS-USDT"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    d = r.json()["data"]
    return dict(ex="kucoin", pair=PAIR_DISPLAY,
                last=float(d["price"]), bid=float(d["bestBid"]),
                ask=float(d["bestAsk"]), ts=int(d["time"]))

def get_bitmart():
    r = requests.get("https://api-cloud.bitmart.com/spot/quotation/v3/ticker",
                     params={"symbol": "KAS_USDT"}, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    d = j.get("data")
    if not isinstance(d, dict):
        raise RuntimeError(f"empty data from bitmart: {j}")
    last = float(d["last"])
    bid = float(d.get("bid_px") or d.get("buy_one"))
    ask = float(d.get("ask_px") or d.get("sell_one"))
    if not (0.01 < last < 1.0) or not (0.01 < bid < 1.0) or not (0.01 < ask < 1.0):
        raise RuntimeError(f"bitmart out-of-range values: last={last}, bid={bid}, ask={ask}")
    ts = int(d.get("ts") or 0)
    return dict(ex="bitmart", pair=PAIR_DISPLAY, last=last, bid=bid, ask=ask, ts=ts)

def get_bitget():
    params = {"symbol": "KASUSDT"}
    try:
        r = requests.get("https://api.bitget.com/api/spot/v1/market/ticker",
                         params=params, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        j = r.json()
        d = j.get("data") or {}
    except Exception:
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

FETCHERS = []
if use_kucoin:  FETCHERS.append(get_kucoin)
if use_gate:    FETCHERS.append(get_gate)
if use_mexc:    FETCHERS.append(get_mexc)
if use_bitmart: FETCHERS.append(get_bitmart)
if use_bitget:  FETCHERS.append(get_bitget)

# -------- Orderbook fetchers --------
def ob_gate(depth):
    url = "https://api.gateio.ws/api/v4/spot/order_book"
    r = requests.get(url, params={"currency_pair": "KAS_USDT", "limit": depth},
                     headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    bids = [(float(p), float(a)) for p, a in j.get("bids", [])]
    asks = [(float(p), float(a)) for p, a in j.get("asks", [])]
    return (pd.DataFrame(bids, columns=["price","amount"]).sort_values("price", ascending=False),
            pd.DataFrame(asks, columns=["price","amount"]).sort_values("price", ascending=True))

def ob_mexc(depth):
    url = "https://api.mexc.com/api/v3/depth"
    r = requests.get(url, params={"symbol": "KASUSDT", "limit": depth},
                     headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    bids = [(float(p), float(q)) for p, q in j.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in j.get("asks", [])]
    return (pd.DataFrame(bids, columns=["price","amount"]).sort_values("price", ascending=False),
            pd.DataFrame(asks, columns=["price","amount"]).sort_values("price", ascending=True))

def ob_kucoin(depth):
    endpoint = "level2_20" if depth <= 20 else "level2_100"
    url = f"https://api.kucoin.com/api/v1/market/orderbook/{endpoint}"
    r = requests.get(url, params={"symbol": "KAS-USDT"}, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    d = j.get("data", {})
    bids = [(float(p), float(sz)) for p, sz in d.get("bids", [])][:depth]
    asks = [(float(p), float(sz)) for p, sz in d.get("asks", [])][:depth]
    return (pd.DataFrame(bids, columns=["price","amount"]).sort_values("price", ascending=False),
            pd.DataFrame(asks, columns=["price","amount"]).sort_values("price", ascending=True))

def ob_bitmart(depth):
    url = "https://api-cloud.bitmart.com/spot/quotation/v3/books"
    r = requests.get(url, params={"symbol": "KAS_USDT", "limit": depth},
                     headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    d = j.get("data") or {}
    bids = [(float(x["price"]), float(x["amount"])) for x in d.get("buys", [])][:depth]
    asks = [(float(x["price"]), float(x["amount"])) for x in d.get("sells", [])][:depth]
    return (pd.DataFrame(bids, columns=["price","amount"]).sort_values("price", ascending=False),
            pd.DataFrame(asks, columns=["price","amount"]).sort_values("price", ascending=True))

def ob_bitget(depth):
    url = "https://api.bitget.com/api/spot/v1/market/depth"
    r = requests.get(url, params={"symbol": "KASUSDT", "type": "step0", "limit": depth},
                     headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    d = j.get("data") or {}
    bids = [(float(p), float(sz)) for p, sz, *_ in d.get("bids", [])][:depth]
    asks = [(float(p), float(sz)) for p, sz, *_ in d.get("asks", [])][:depth]
    return (pd.DataFrame(bids, columns=["price","amount"]).sort_values("price", ascending=False),
            pd.DataFrame(asks, columns=["price","amount"]).sort_values("price", ascending=True))

def orderbook_kpis(bids_df: pd.DataFrame, asks_df: pd.DataFrame):
    """Calcule KPIs instantanés (Niveau 1) pour un carnet."""
    if bids_df.empty or asks_df.empty:
        return None
    best_bid = float(bids_df["price"].max())
    best_ask = float(asks_df["price"].min())
    mid = (best_bid + best_ask) / 2.0
    spread_pct = ((best_ask - best_bid) / mid) * 100 if mid else 0.0

    depth_buy = float(bids_df["amount"].sum())
    depth_sell = float(asks_df["amount"].sum())
    total_depth = depth_buy + depth_sell
    imbalance = ((depth_buy - depth_sell) / total_depth) if total_depth else 0.0

    # Indice simple de "qualité de marché"
    liq_index = (total_depth / spread_pct) if spread_pct > 0 else float("inf")
    return {
        "best_bid": best_bid, "best_ask": best_ask, "mid": mid,
        "spread_pct": spread_pct, "depth_buy": depth_buy, "depth_sell": depth_sell,
        "total_depth": total_depth, "imbalance": imbalance, "liquidity_index": liq_index,
    }

# Mémoire pour les tendances (Niveau 2)
if "ob_prev" not in st.session_state:
    st.session_state["ob_prev"] = {}   # ex -> {"mid":..., "imbalance":..., "total_depth":...}

ORDERBOOK_FETCHERS = {
    "gate": ob_gate,
    "mexc": ob_mexc,
    "kucoin": ob_kucoin,
    "bitmart": ob_bitmart,
    "bitget": ob_bitget,
}

# -------- Core --------
def collect_once():
    asof = datetime.now(timezone.utc)
    quotes, errors = [], []

    for fn in FETCHERS:
        try:
            q = fn()
            q["mid"] = (q["bid"] + q["ask"]) / 2.0
            q["ts"] = q["ts"] or int(asof.timestamp() * 1000)
            quotes.append(q)
        except Exception as e:
            errors.append(f"{fn.__name__}: {e}")

    if len(quotes) >= 2:
        mids = [q["mid"] for q in quotes]
        provisional_med = median(mids)
        threshold = outlier_pct / 100.0
        kept, dropped = [], []
        for q in quotes:
            if abs(q["mid"] - provisional_med) / provisional_med <= threshold:
                kept.append(q)
            else:
                dropped.append(q)
        if dropped and kept:
            errors.append("outliers dropped (>{:.1f}%): {}".format(
                outlier_pct, ", ".join(f"{q['ex']}={q['mid']:.6f}" for q in dropped)))
            quotes = kept

    if quotes:
        med = median([q["mid"] for q in quotes])
        spread_bps = 10000.0 * (max(q["mid"] for q in quotes) - min(q["mid"] for q in quotes)) / med
    else:
        med, spread_bps = None, None

    return {"asof_iso": asof.isoformat(timespec="seconds"),
            "quotes": quotes, "median": med,
            "spread_max_bps": spread_bps, "errors": errors}

# ---- UI controls ----
col1, col2 = st.columns([1, 1])
run_now = col1.button("▶ Collecter maintenant", type="primary")
auto = col2.checkbox("Auto-refresh (activer)", value=False, help="Utilise l'intervalle choisi dans la sidebar.")
out_placeholder = st.empty()

def csv_bytes_from_quotes(payload):
    quotes = payload.get("quotes", [])
    if not quotes:
        return b""
    df = pd.DataFrame(quotes)[["ex","pair","last","bid","ask","mid","ts"]]
    return df.to_csv(index=False).encode("utf-8")

def csv_bytes_from_summary(payload):
    row = {"timestamp_iso": payload["asof_iso"],
           "median_mid": payload["median"],
           "spread_max_bps": payload["spread_max_bps"],
           "sources_ok": len(payload["quotes"])}
    for q in payload["quotes"]:
        row[f"{q['ex']}_last"] = q["last"]
    df = pd.DataFrame([row])
    return df.to_csv(index=False).encode("utf-8")

def render_once():
    payload = collect_once()
    quotes = payload["quotes"]
    df = pd.DataFrame(quotes) if quotes else pd.DataFrame(columns=["ex","pair","last","bid","ask","mid","ts"])

    out_placeholder.subheader("🧾 Quotes")
    out_placeholder.dataframe(df[["ex","pair","last","bid","ask","mid","ts"]].sort_values("ex"),
                              width="stretch", hide_index=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Médiane (mid)", f"{payload['median']:.8f}" if payload["median"] else "—")
    c2.metric("Écart max (bps)", f"{payload['spread_max_bps']:.2f}" if payload["spread_max_bps"] is not None else "—")
    c3.metric("Sources valides", str(len(quotes)))
    st.caption(f"asof (UTC) = {payload['asof_iso']}")

    if payload["errors"]:
        for msg in payload["errors"]:
            st.warning(msg)

    # --- Orderbooks ---
    global_best = []  # pour le comparatif inter-exchanges (Niveau 3)
    if show_ob and quotes:
        st.subheader(f"📚 Orderbooks (profondeur {ob_depth})")

        # onglets uniquement pour les exchanges qui ont un fetcher d'orderbook
        ex_list = [q["ex"] for q in quotes if q["ex"] in ORDERBOOK_FETCHERS]
        tabs = st.tabs([ex.upper() for ex in ex_list])

        for tab, ex in zip(tabs, ex_list):
            with tab:
                try:
                    bids_df, asks_df = ORDERBOOK_FETCHERS[ex](ob_depth)

                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("**Bids (acheteurs)**")
                        st.dataframe(bids_df, width="stretch", hide_index=True)
                    with c2:
                        st.markdown("**Asks (vendeurs)**")
                        st.dataframe(asks_df, width="stretch", hide_index=True)

                    # KPIs Niveau 1
                    k = orderbook_kpis(bids_df, asks_df)
                    if k is None:
                        st.info("Carnet vide.")
                        continue

                    # KPIs Niveau 2 : tendances vs run précédent
                    prev = st.session_state["ob_prev"].get(ex, {})
                    delta_mid = (k["mid"] - prev.get("mid")) if "mid" in prev else 0.0
                    delta_imb = (k["imbalance"] - prev.get("imbalance")) if "imbalance" in prev else 0.0
                    delta_depth = (k["total_depth"] - prev.get("total_depth")) if "total_depth" in prev else 0.0

                    st.markdown("**KPIs**")
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Spread (%)", f"{k['spread_pct']:.3f}")
                    m2.metric("Imbalance", f"{k['imbalance']:.2f}", delta=f"{delta_imb:+.2f}")
                    m3.metric("Total depth", f"{k['total_depth']:.0f}", delta=f"{delta_depth:+.0f}")
                    m4.metric("Mid", f"{k['mid']:.6f}", delta=f"{delta_mid:+.6f}")

                    # stocke pour run suivant
                    st.session_state["ob_prev"][ex] = {
                        "mid": k["mid"], "imbalance": k["imbalance"], "total_depth": k["total_depth"]
                    }

                    # pour comparatif inter-exchanges (Niveau 3)
                    global_best.append({"ex": ex, "best_bid": k["best_bid"], "best_ask": k["best_ask"],
                                        "mid": k["mid"], "liq": k["liquidity_index"]})

                except Exception as e:
                    st.warning(f"{ex}: orderbook indisponible — {e}")

        # KPIs Niveau 3 : comparatif inter-exchanges (si ≥2 carnets)
        if len(global_best) >= 2:
            bb = max(global_best, key=lambda x: x["best_bid"])
            ba = min(global_best, key=lambda x: x["best_ask"])
            cross_mid = (bb["best_bid"] + ba["best_ask"]) / 2.0
            cross_spread_pct = ((ba["best_ask"] - bb["best_bid"]) / cross_mid) * 100 if cross_mid else 0.0
            cross_spread_bps = cross_spread_pct * 100

            st.markdown("### 🌐 KPIs inter-exchanges")
            c1, c2, c3 = st.columns(3)
            c1.metric("Best Bid", f"{bb['best_bid']:.6f}", help=f"Exchange: {bb['ex'].upper()}")
            c2.metric("Best Ask", f"{ba['best_ask']:.6f}", help=f"Exchange: {ba['ex'].upper()}")
            c3.metric("Spread global", f"{cross_spread_pct:.3f}% ({cross_spread_bps:.1f} bps)")

            # Classement "liquidity index" (simple)
            liq_rank = sorted(global_best, key=lambda x: x["liq"], reverse=True)
            st.caption("Liquidity index (↑ meilleur)")
            st.table(pd.DataFrame([{"exchange": x["ex"].upper(),
                                    "mid": f"{x['mid']:.6f}",
                                    "liquidity_index": (f"{x['liq']:.0f}" if x['liq'] != float('inf') else "∞")} 
                                   for x in liq_rank]))

    # --- Téléchargements ---
    ts_safe = payload["asof_iso"].replace(":", "-")
    st.download_button("⬇️ Télécharger les quotes (CSV)",
                       data=csv_bytes_from_quotes(payload),
                       file_name=f"kas_quotes_{ts_safe}.csv",
                       mime="text/csv", type="secondary")
    st.download_button("⬇️ Télécharger la ligne synthèse (CSV)",
                       data=csv_bytes_from_summary(payload),
                       file_name=f"kas_summary_{ts_safe}.csv",
                       mime="text/csv", type="secondary")

if run_now:
    render_once()

if auto and interval > 0:
    render_once()
    st.caption(f"Auto-refresh actif : toutes les {interval} s.")
    time.sleep(interval)
    st.rerun()
