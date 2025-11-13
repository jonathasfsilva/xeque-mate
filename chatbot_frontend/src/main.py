import os
import requests
import streamlit as st
import json
from datetime import datetime
import threading
import time
from queue import Queue
from streamlit.components.v1 import html as st_html

# Try to import st_autorefresh to force periodic reruns (so queued alerts get processed)
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

# Configuration
CHATBOT_URL = os.getenv("CHATBOT_URL")

# Thread-safe queue for alerts from polling thread
alert_queue = Queue()

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "last_processed_alert_ts" not in st.session_state:
    st.session_state.last_processed_alert_ts = None

if "alert_poller_running" not in st.session_state:
    st.session_state.alert_poller_running = False

if "processed_alert_ts" not in st.session_state:
    # set of timestamps we've already added to the chat (for deduplication)
    st.session_state.processed_alert_ts = set()

# Sidebar: manual fetch button to deterministically inject alerts into chat
with st.sidebar:
    st.header("Alertas")
    if st.button("Checar alertas agora"):
        try:
            resp = requests.get(f"{CHATBOT_URL}/alerts/recent?limit=50", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                alerts = data.get("alerts", [])
                added = 0
                # process oldest -> newest
                for a in reversed(alerts):
                    ts = a.get("timestamp")
                    if not ts:
                        # if no timestamp, always add once using title+message key
                        key = (a.get("title"), a.get("message"))
                        ts = json.dumps(key)
                    if ts not in st.session_state.processed_alert_ts:
                        st.session_state.processed_alert_ts.add(ts)
                        severity = a.get("severity", "info").upper()
                        title = a.get("title", "Alerta")
                        message = a.get("message", "")
                        st.session_state.messages.append({
                            "role": "assistant",
                            "output": f"🔔 **[{severity}]** {title}: {message}",
                            "is_alert": True,
                            "timestamp": ts,
                        })
                        added += 1
                if added:
                    st.success(f"{added} alerta(s) adicionados ao chat")
                else:
                    st.info("Nenhum alerta novo encontrado")
            else:
                st.error(f"Erro ao buscar alertas: HTTP {resp.status_code}")
        except Exception as e:
            st.error(f"Erro ao buscar alertas: {e}")


def poll_alerts():
    """Background thread that polls for new alerts."""
    while True:
        try:
            response = requests.get(f"{CHATBOT_URL}/alerts/recent?limit=20", timeout=5)
            if response.status_code == 200:
                data = response.json()
                alerts = data.get("alerts", [])
                for alert in alerts:
                    # Do not access Streamlit session_state from background thread.
                    # Just enqueue alerts and let the main thread deduplicate and process them.
                    try:
                        alert_queue.put(alert)
                        print(f"[poll_alerts] enqueued alert: {alert.get('title')}")
                    except Exception as e:
                        print(f"[poll_alerts] failed to enqueue alert: {e}")
            time.sleep(2.5)
        except Exception:
            time.sleep(2.5)


# Start alert polling thread once per session
if not st.session_state.alert_poller_running:
    st.session_state.alert_poller_running = True
    thread = threading.Thread(target=poll_alerts, daemon=True)
    thread.start()

# ============================================================================
# Process queued alerts and add them to messages
# ============================================================================
while not alert_queue.empty():
    try:
        alert = alert_queue.get_nowait()
        ts = alert.get("timestamp")
        if st.session_state.last_processed_alert_ts != ts:
            st.session_state.last_processed_alert_ts = ts
            severity = alert.get("severity", "info").upper()
            title = alert.get("title", "Alerta")
            message = alert.get("message", "")
            print(f"[main] processing alert: {title} ts={ts}")
            # Insert alert into chat as if it were an assistant response
            st.session_state.messages.append({
                "role": "assistant",
                "output": f"🔔 **[{severity}]** {title}: {message}",
                "is_alert": True,
                "timestamp": ts,
            })
    except Exception:
        pass

# Force periodic reruns so the main loop drains the alert_queue regularly.
if st_autorefresh is not None:
    st_autorefresh(interval=2500, key="alerts_autorefresh")

# Also open a small browser-side WebSocket component to receive immediate alerts
# and post them to Streamlit via postMessage. This ensures alerts appear
# in the chat UI as soon as they're broadcast by the backend.
ws_html = """
<script>
(function(){
    var host = window.location.hostname || 'localhost';
    var proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
    var wsUrl = proto + '://' + host + ':8000/ws/alerts';
    try{
        var socket = new WebSocket(wsUrl);
        socket.onmessage = function(e){
            try{ var data = JSON.parse(e.data); window.parent.postMessage({isStreamlitMessage: true, type: 'streamlit:setComponentValue', value: JSON.stringify(data)}, '*'); }catch(err){}
        };
    }catch(err){}
})();
</script>
"""

# The component returns the last value posted via postMessage (stringified JSON)
last_alert = st_html(ws_html, height=0)
if last_alert:
    try:
        alert_obj = json.loads(last_alert) if isinstance(last_alert, str) else last_alert
        ts = alert_obj.get('timestamp')
        if st.session_state.last_processed_alert_ts != ts:
            st.session_state.last_processed_alert_ts = ts
            severity = alert_obj.get('severity', 'info').upper()
            title = alert_obj.get('title', 'Alerta')
            message = alert_obj.get('message', '')
            # Insert alert into chat as assistant message so it appears like a response
            st.session_state.messages.append({
                'role': 'assistant',
                'output': f"🔔 **[{severity}]** {title}: {message}",
                'is_alert': True,
                'timestamp': ts
            })
    except Exception:
        pass

# ============================================================================
# MAIN CHAT INTERFACE
# ============================================================================
st.title("Chatbot de CTI - Ransomware")
st.info("Pergunte-me sobre atores de ameaça, ransomware, vítimas e suas relações!")

# Display all messages (user, assistant, and alerts)
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["output"])
        
        if "explanation" in message and message["explanation"]:
            with st.status("Como isso foi gerado", state="complete"):
                st.info(message["explanation"])


# Chat input and processing
if prompt := st.chat_input("O que você quer saber?"):
    # Add and display user message
    st.session_state.messages.append({"role": "user", "output": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Fetch response from agent
    try:
        with st.spinner("Analisando o grafo de ameaças..."):
            response = requests.post(
                f"{CHATBOT_URL}/rag-agent",
                json={"text": prompt},
                timeout=300
            )
        
        if response.status_code == 200:
            result = response.json()
            output_text = result.get("output", "")
            explanation = result.get("intermediate_steps", "")
        else:
            output_text = "Ocorreu um erro ao processar sua mensagem. Tente novamente ou reformule sua pergunta."
            explanation = ""
    except Exception as e:
        output_text = f"Erro ao conectar com o agente: {str(e)}"
        explanation = ""
    
    # Display and store assistant response
    with st.chat_message("assistant"):
        st.markdown(output_text)
    
    if explanation:
        with st.status("Como isso foi gerado?", state="complete"):
            st.info(explanation)
    
    st.session_state.messages.append({
        "role": "assistant",
        "output": output_text,
        "explanation": explanation,
    })

