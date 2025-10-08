# app.py
import os
import re
import json
import streamlit as st
from openai import OpenAI

# -----------------------
# CONFIG
# -----------------------
MODEL = "gpt-4.1"
WORKFLOW_ID = "wf_68e664370e5081909776a87aba127ed602bdfb4"
API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))

st.set_page_config(page_title="KAS Price Oracle — AgentKit x Streamlit", layout="centered")
st.title("KAS Price Oracle — AgentKit ▶ Streamlit (Debug)")

if not API_KEY:
    st.error("⚠️ OPENAI_API_KEY manquante. Ajoute-la dans Settings → Secrets ou en variable d'environnement.")
    st.stop()

client = OpenAI(api_key=API_KEY)

st.caption("Cette page déclenche ton **workflow AgentKit** publié et affiche la réponse brute, puis en JSON si possible.")


# -----------------------
# UTILS
# -----------------------
def extract_json(text: str):
    """Essaie de parser un JSON depuis un texte arbitraire."""
    if not text:
        return None
    # 1) tentative direct
    try:
        return json.loads(text)
    except Exception:
        pass
    # 2) chercher un bloc JSON dans le texte
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None


# -----------------------
# CORE: appel du workflow (sans retry, 4 variantes)
# -----------------------
def run_workflow_debug():
    """Tente plusieurs signatures d'appel du workflow selon la version du SDK,
    et renvoie (raw_text, data_json | None). Affiche les erreurs si toutes échouent.
    """
    user_input = [{"role": "user", "content": "Run KAS collection"}]
    headers = {"OpenAI-Beta": "workflows=v1"}
    variants = [
        ("workflow_kw", dict(workflow=WORKFLOW_ID)),  # certaines versions acceptent ce kw
        ("extra_body.workflow", dict(extra_body={"workflow": WORKFLOW_ID})),
        ("extra_body.workflow_id", dict(extra_body={"workflow_id": WORKFLOW_ID})),
        ("extra_body.workflow+version", dict(extra_body={"workflow": WORKFLOW_ID, "version": "1"})),
    ]

    errors = []
    for name, kwargs in variants:
        try:
            resp = client.responses.create(
                model=MODEL,
                input=user_input,
                extra_headers=headers,
                **kwargs,
            )
            # Plusieurs formes de retour existent selon versions
            text = getattr(resp, "output_text", None)
            if not text:
                # essaye via model_dump si dispo (pydantic)
                try:
                    text = json.dumps(resp.model_dump(), ensure_ascii=False)
                except Exception:
                    text = str(resp)

            data = extract_json(text)
            return text, data
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {e}")

    # Si rien n'a marché, on affiche tout pour débogage
    st.error("❌ Toutes les variantes d'appel du workflow ont échoué.")
    st.code("\n".join(errors))
    raise RuntimeError("Workflow call failed")


# -----------------------
# UI
# -----------------------
col1, col2 = st.columns([1,1])
with col1:
    click = st.button("▶ Collecter maintenant", type="primary")
with col2:
    auto = st.checkbox("Auto-refresh (10 s)")

def do_run():
    with st.spinner("Appel du workflow AgentKit…"):
        raw_text, data = run_workflow_debug()

    st.subheader("Réponse brute")
    st.code(raw_text)

    if data:
        st.subheader("Interprétation JSON")
        st.json(data)
    else:
        st.info("Aucun JSON valide détecté dans la réponse (normal en phase debug).")

if click:
    do_run()

if auto:
    import time
    do_run()
    st.caption("Auto-refresh actif (toutes les 10 s). Désactive la case pour arrêter.")
    time.sleep(10)
    st.rerun()
