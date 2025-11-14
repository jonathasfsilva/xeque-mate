import os
import json
import time
import threading
import requests
import streamlit as st
from queue import Queue
from datetime import datetime
from streamlit.components.v1 import html as st_html
from streamlit_autorefresh import st_autorefresh

# ================================================================
# CONFIGURAÇÕES
# ================================================================
CHATBOT_URL = os.getenv("CHATBOT_URL")
ALERT_INTERVAL = 10_000  # autorefresh a cada 10s

# ================================================================
# SESSION STATE
# ================================================================
if "messages" not in st.session_state:
    st.session_state.messages = []

if "processed_alerts" not in st.session_state:
    st.session_state.processed_alerts = set()

if "is_processing" not in st.session_state:
    st.session_state.is_processing = False

if "alert_thread_running" not in st.session_state:
    st.session_state.alert_thread_running = False

# Fila thread-safe para alertas vindos do backend
alert_queue = st.session_state.get("alert_queue") or Queue()
st.session_state.alert_queue = alert_queue


# ================================================================
# FUNÇÃO DE POLLING EM THREAD (SEGUNDO PLANO)
# ================================================================
def poll_alerts_thread():
    while True:
        try:
            resp = requests.get(f"{CHATBOT_URL}/alerts/recent?limit=20", timeout=5)
            if resp.status_code == 200:
                alerts = resp.json().get("alerts", [])
                for alert in alerts:
                    alert_queue.put(alert)
        except:
            pass
        time.sleep(3)


# iniciar thread somente uma vez
if not st.session_state.alert_thread_running:
    st.session_state.alert_thread_running = True
    t = threading.Thread(target=poll_alerts_thread, daemon=True)
    t.start()


# ================================================================
# DRENAR ALERTAS DO QUEUE (MAIN THREAD)
# ================================================================
def process_alert_queue():
    while not alert_queue.empty():
        try:
            alert = alert_queue.get_nowait()
            ts = alert.get("timestamp") or json.dumps(alert)

            if ts not in st.session_state.processed_alerts:
                st.session_state.processed_alerts.add(ts)

                severity = alert.get("severity", "info").upper()
                title = alert.get("title", "Alerta")
                message = alert.get("message", "")

                st.session_state.messages.append({
                    "role": "assistant",
                    "output": f"🔔 **[{severity}]** {title}: {message}",
                    "timestamp": ts,
                    "is_alert": True
                })
        except:
            pass


process_alert_queue()


# ================================================================
# WEBSOCKET (CLIENTE NO NAVEGADOR)
# ================================================================
ws_html = """
<script>
(function(){
    let proto = window.location.protocol === "https:" ? "wss" : "ws";
    let url = proto + "://" + window.location.hostname + ":8000/ws/alerts";

    try{
        let socket = new WebSocket(url);
        socket.onmessage = function(event){
            try {
                let d = JSON.parse(event.data);
                window.parent.postMessage(
                  { isStreamlitMessage: true, type: "streamlit:setComponentValue", value: JSON.stringify(d) },
                  "*"
                );
            } catch(e){}
        }
    } catch(e){}
})();
</script>
"""

ws_data = st_html(ws_html, height=0)

# processar alerta via WebSocket
if ws_data:
    try:
        alert = json.loads(ws_data)
        ts = alert.get("timestamp") or json.dumps(alert)

        if ts not in st.session_state.processed_alerts:
            st.session_state.processed_alerts.add(ts)

            severity = alert.get("severity", "info").upper()
            title = alert.get("title", "Alerta")
            message = alert.get("message", "")

            st.session_state.messages.append({
                "role": "assistant",
                "output": f"🔔 **[{severity}]** {title}: {message}",
                "timestamp": ts,
                "is_alert": True
            })
    except:
        pass


# ================================================================
# AUTOREFRESH SEGURO (não interrompe requisições longas)
# ================================================================
if not st.session_state.is_processing:
    st_autorefresh(interval=ALERT_INTERVAL, key="safe_autorefresh")


# ================================================================
# UI PRINCIPAL
# ================================================================
st.title("Chatbot de CTI - Ransomware")
st.info("Pergunte sobre atores de ameaça, ransomware, vítimas e relações.")

# --------------------------------------
# Mostrar todas as mensagens
# --------------------------------------
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["output"])

# ================================================================
# INPUT DO CHAT
# ================================================================
prompt = st.chat_input("O que você quer saber?")

if prompt:
    st.session_state.is_processing = True

    # mostrar usuário
    st.session_state.messages.append({"role": "user", "output": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # gerar resposta
    try:
        with st.spinner("Analisando o grafo de ameaças..."):
            r = requests.post(
                f"{CHATBOT_URL}/rag-agent",
                json={"text": prompt},
                timeout=300
            )

        if r.status_code == 200:
            data = r.json()
            output = data.get("output", "")
            explanation = data.get("intermediate_steps", "")
        else:
            output = "Erro ao processar sua mensagem."
            explanation = ""

        with st.chat_message("assistant"):
            st.markdown(output)

        st.session_state.messages.append({
            "role": "assistant",
            "output": output,
            "explanation": explanation
        })

    except Exception as e:
        with st.chat_message("assistant"):
            st.markdown(f"Erro ao conectar: {e}")

    finally:
        st.session_state.is_processing = False
