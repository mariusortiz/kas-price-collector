import os, json, re, time
import pandas as pd
import streamlit as st
from tenacity import retry, wait_exponential, stop_after_attempt
from openai import OpenAI

# ------- CONFIG -------
WORKFLOW_ID = "wf_68e664370e5081909776a87aba127ed602bdfb4"  # <-- ton ID
MODEL = "gpt-4.1"  # modèle 'driver' du workflow
# ----------------------

# API key depuis st.secrets ou env
API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
if not API_KEY:
    st.error("⚠️ Configure OPENAI_API_KEY dans .streamlit/secrets.toml ou variable d'environnement.")
    st.stop()

client = OpenAI(api_key=API_KEY)

st.set_page_config(page_title="KAS Price Oracle (AgentKit + Streamlit)", layout="centered")
st.title("KAS Price Oracle — AgentKit ▶ Streamlit")

# Sidebar: auto refresh
st.sidebar.header("Options")
interval = st.sidebar.slider("Auto-refresh (sec)", 0, 60, 0, help="0 = pas d’auto-refresh")
max_sources = st.sidebar.number_input("Nb min. de sources valides", min_value=1, max_value=10, value=2, step=1)

# Petit util pour extraire du JSON quelle que soit la forme
def extract_json_from_text(text: str):
    try:
        return json.loads(text)
    except Exception:
        # Cherche le premier bloc JSON plausible
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return None

@retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
def run_workflow():
    # Appel direct du workflow AgentKit via Responses API
    resp = client.responses.create(
        model=MODEL,
        workflow=WORKFLOW_ID,
        input=[{"role": "user", "content": "Run KAS collection"}],
    )

    # Plusieurs SDKs exposent output_text; on gère aussi les structures alternatives
    text = getattr(resp, "output_text", None)
    if not text and hasattr(resp, "output"):
        # concatène tous les morceaux de texte s'il y en a
        try:
            text = "".join([
                (c.get("text", {}).get("value") if isinstance(c, dict) else "")
                for o in resp.output for c in getattr(o, "content", [])
            ])
        except Exception:
            text = None

    if not text:
        text = str(resp)

    data = extract_json_from_text(text)
    return text, data

placeholder = st.empty()

def render_once():
    with st.spinner("Collecte en cours depuis le workflow AgentKit…"):
        raw_text, data = run_workflow()

    if not data:
        st.error("Réponse non JSON. Affichage brut ci-dessous :")
        st.code(raw_text)
        return

    # Validation minimum
    quotes = data.get("quotes", [])
    median = data.get("median")
    spread = data.get("spread_max_bps")
    asof = data.get("asof")

    # Filtrage sources valides
    df = pd.DataFrame(quotes)
    if not df.empty:
        # colonnes attendues
        for col in ["ex","pair","last","bid","ask","ts"]:
            if col not in df.columns:
                df[col] = None

        st.subheader("🧾 Quotes")
        st.dataframe(
            df[["ex","pair","last","bid","ask","ts"]].sort_values("ex"),
            use_container_width=True,
            hide_index=True
        )

    cols = st.columns(3)
    cols[0].metric("Médiane (mid)", f"{median:.8f}" if isinstance(median,(int,float)) else "—")
    cols[1].metric("Écart max (bps)", f"{spread:.2f}" if isinstance(spread,(int,float)) else "—")
    cols[2].metric("Sources reçues", str(len(quotes)))

    st.caption(f"asof = {asof}")

    # Statut
    status = "✅ OK" if len(quotes) >= max_sources and isinstance(spread,(int,float)) else "🟠 Dégradé"
    st.info(f"Statut agrégé : {status}")

    with st.expander("Réponse JSON complète"):
        st.code(json.dumps(data, indent=2, ensure_ascii=False))

render_once()

# Auto-refresh basique
if interval and interval > 0:
    st.caption(f"Auto-refresh toutes les {interval}s (désactive via la sidebar).")
    time.sleep(interval)
    st.rerun()
