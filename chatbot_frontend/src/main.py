import os
import requests
import streamlit as st

# 1. ATUALIZADO: O endpoint da sua nova API
CHATBOT_URL = os.getenv("CHATBOT_URL")

with st.sidebar:
    st.header("Sobre")
    # 2. ATUALIZADO: Descrição do novo chatbot
    st.markdown(
        """
        Este chatbot interage com um agente
        [LangChain](https://python.langchain.com/docs/get_started/introduction)
        projetado para responder a perguntas sobre Cyber Threat Intelligence (CTI)
        com foco em ransomware.
        
        Ele responde a perguntas sobre **atores de ameaça** (grupos),
        **famílias de ransomware**, **vítimas**, **setores da indústria** e **carteiras**
        de pagamento. O agente utiliza geração de recuperação e ampliação (RAG)
        sobre um grafo de conhecimento estruturado.
        """
    )

    st.header("Exemplos de perguntas")
    # 3. ATUALIZADO: Novos exemplos de perguntas para CTI
    st.markdown("- Quais ransomware o grupo Wizard Spider opera?")
    st.markdown("- Qual a motivação do Lazarus Group?")
    st.markdown("- Quantas vítimas o ransomware Conti atacou no total?")
    st.markdown("- Quais setores (indústrias) o ransomware LockBit atacou?")
    st.markdown("- Qual grupo opera o ransomware Ryuk?")
    st.markdown("- Liste todas as vítimas no setor de 'Energia'.")
    st.markdown("- Qual a família do ransomware 'DarkSide'?")
    st.markdown("- Quais grupos têm motivação 'Financial' (Financeira)?")
    st.markdown("- Quais carteiras o ransomware Conti usa para pedir resgate?")


# 4. ATUALIZADO: Título
st.title("Chatbot de CTI - Ransomware")
# 5. ATUALIZADO: Info
st.info(
    """Pergunte-me sobre atores de ameaça, ransomware, vítimas e suas relações!"""
)

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
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
        response = requests.post(CHATBOT_URL, json=data)

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