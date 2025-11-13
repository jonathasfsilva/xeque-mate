import os
import requests
import streamlit as st
import json
from datetime import datetime

# 1. ATUALIZADO: O endpoint da sua nova API
CHATBOT_URL = os.getenv("CHATBOT_URL")

# We'll use a small browser-side WebSocket (embedded HTML/JS)
from streamlit.components.v1 import html as st_html


with st.sidebar:
        st.header("Status de Alertas")

        # Browser-side WebSocket widget (connects from the user's browser to the API)
        html_code = """
        <div id="alert-box" style="font-family: sans-serif; padding:8px; border-radius:6px; border:1px solid #ddd;">
            <b>Status:</b> <span id="status">Conectando...</span>
            <div id="alerts" style="margin-top:8px; max-height:220px; overflow:auto;"></div>
        </div>
        <script>
        (function() {{
                        // Determine a safe host: try window.location, then parent frame, then fallback to localhost
                        var host = window.location.hostname || (window.parent && window.parent.location && window.parent.location.hostname) || 'localhost';
                        var proto = (window.parent && window.parent.location && window.parent.location.protocol) || window.location.protocol || 'http:';
                        const wsProto = proto === 'https:' ? 'wss' : 'ws';
                        const wsUrl = wsProto + '://' + host + ':8000/ws/alerts';
                        console.debug('[alerts widget] connecting to', wsUrl);
                        var socket = null;
                        try {
                            socket = new WebSocket(wsUrl);
                        } catch (e) {
                            console.error('[alerts widget] websocket construction error', e);
                            document.getElementById('status').textContent = 'Erro na construção do WebSocket';
                        }
            const status = document.getElementById('status');
            const alertsDiv = document.getElementById('alerts');
            socket.onopen = function() {{ status.textContent = 'Conectado'; }};
            socket.onmessage = function(event) {{
                try {{
                    const data = JSON.parse(event.data);
                    const el = document.createElement('div');
                    el.style.padding = '6px';
                    el.style.borderTop = '1px solid #eee';
                    el.innerHTML = '<b>' + (data.title||'Alerta') + '</b><div>' + (data.message||'') + '</div>';
                    alertsDiv.prepend(el);
                }} catch(e) {{ console.error(e); }}
            }};
            socket.onclose = function() {{ status.textContent = 'Desconectado'; }};
            socket.onerror = function() {{ status.textContent = 'Erro'; }};
        }})();
        </script>
        """

        st_html(html_code, height=220)


# 4. ATUALIZADO: Título
st.title("Chatbot de CTI - Ransomware")
# 5. ATUALIZADO: Info
st.info(
    """Pergunte-me sobre atores de ameaça, ransomware, vítimas e suas relações!"""
)

if "messages" not in st.session_state:
    st.session_state.messages = []

if "last_alert" not in st.session_state:
    st.session_state.last_alert = None

# Exibir alerta mais recente na tela principal
if st.session_state.last_alert:
    alert = st.session_state.last_alert
    severity = alert.get("severity", "info")
    title = alert.get("title", "Alerta")
    message = alert.get("message", "")
    
    if severity == "error":
        st.error(f"🚨 **{title}**\n{message}")
    elif severity == "warning":
        st.warning(f"⚠️ **{title}**\n{message}")
    elif severity == "success":
        st.success(f"✅ **{title}**\n{message}")
    else:
        st.info(f"ℹ️ **{title}**\n{message}")
    
    # Adicionar alerta ao histórico de chat se ainda não foi adicionado
    if not any(msg.get("is_alert") for msg in st.session_state.messages if msg.get("title") == title):
        st.session_state.messages.append({
            "role": "system",
            "output": f"**[{severity.upper()}]** {title}: {message}",
            "is_alert": True,
            "title": title
        })


for message in st.session_state.messages:
    # Pular mensagens de alerta no loop de exibição (já foram mostradas acima)
    if message.get("is_alert"):
        continue
        
    with st.chat_message(message["role"]):
        if "output" in message.keys():
            st.markdown(message["output"])

        if "explanation" in message.keys():
            with st.status("Como isso foi gerado", state="complete"):
                st.info(message["explanation"])

if prompt := st.chat_input("O que você quer saber?"):
    st.chat_message("user").markdown(prompt)

    st.session_state.messages.append({"role": "user", "output": prompt})

    data = {"text": prompt}

    with st.spinner("Analisando o grafo de ameaças..."):
        response = requests.post(f"{CHATBOT_URL}/rag-agent", json=data)

        if response.status_code == 200:
            output_text = response.json()["output"]
            # A 'explanation' agora são os 'intermediate_steps' do agente
            explanation = response.json()["intermediate_steps"]

        else:
            output_text = """Ocorreu um erro ao processar sua mensagem. Isso geralmente significa que o chatbot falhou ao gerar uma consulta Cypher para responder à sua pergunta. Tente novamente ou reformule sua mensagem."""
            explanation = output_text

    st.chat_message("assistant").markdown(output_text)
    st.status("Como isso foi gerado?", state="complete").info(explanation)

    st.session_state.messages.append(
        {
            "role": "assistant",
            "output": output_text,
            "explanation": explanation,
        }
    )
