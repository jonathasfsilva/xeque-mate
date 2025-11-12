import os
from typing import Any
from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents.format_scratchpad.openai_tools import (
    format_to_openai_tool_messages,
)
from langchain.agents.output_parsers.openai_tools import OpenAIToolsAgentOutputParser

# 1. Importar a nova CADEIA DE CYPHER de CTI
from src.chains.cypher_chain import cypher_chain
from src.chains.review_chain import cti_reports_vector_chain

# (NOTA: As outras cadeias e ferramentas foram removidas, 
# pois não temos os dados de "relatórios" ou "APIs externas" 
# neste momento)

# 2. Renomear a variável do modelo
CTI_AGENT_MODEL = os.getenv("CTI_AGENT_MODEL", "gpt-4") # Use o modelo que preferir

agent_chat_model = ChatOpenAI(
    model=CTI_AGENT_MODEL,
    temperature=0,
)

# 3. Definir as ferramentas
@tool
def consultar_grafo_cti(pergunta: str) -> str:
    """
    Útil para responder perguntas factuais sobre atores de ameaça
    (Threat Actors), famílias de ransomware, vítimas, carteiras
    (wallets), estatísticas de ataques e os relacionamentos entre
    eles. Use a pergunta inteira como entrada para a ferramenta.
    Por exemplo, se a pergunta for "Quantas vítimas o ransomware
    Conti atacou?", a entrada deve ser "Quantas vítimas o 
    ransomware Conti atacou?".
    """
    # Aponta para a cadeia Text-to-Cypher que você criou
    return cypher_chain.invoke(pergunta)

@tool
def buscar_relatorios_de_inteligencia(pergunta: str) -> str:
    """
    Útil quando você precisar responder perguntas qualitativas sobre
    o 'modus operandi' (TTPs), comportamento, motivações ou
    análises detalhadas de um ator de ameaça ou ransomware
    (ex: 'Como o Wizard Spider opera?', 'Quais são os TTPs do Cl0p?').
    """
    return cti_reports_vector_chain.invoke(pergunta)


# 4. Atualizar a lista de ferramentas
agent_tools = [
    consultar_grafo_cti, buscar_relatorios_de_inteligencia
]

# 5. Atualizar o PROMPT DO SISTEMA
agent_prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """
            Você é um chatbot assistente de CTI (Cyber Threat Intelligence),
            projetado para responder perguntas sobre ameaças de ransomware.
            Use o grafo de conhecimento para encontrar informações sobre
            atores de ameaça (Threat Actors), famílias de ransomware,
            vítimas, indústrias atacadas e carteiras de pagamento.
            """,
        ),
        ("user", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]
)

# 6. Vincular o modelo às novas ferramentas
agent_llm_with_tools = agent_chat_model.bind_tools(agent_tools)

# 7. Renomear o agente
cti_rag_agent = (
    {
        "input": lambda x: x["input"],
        "agent_scratchpad": lambda x: format_to_openai_tool_messages(
            x["intermediate_steps"]
        ),
    }
    | agent_prompt
    | agent_llm_with_tools
    | OpenAIToolsAgentOutputParser()
)

# 8. Renomear o executor do agente
rag_agent_executor = AgentExecutor(
    agent=cti_rag_agent,
    tools=agent_tools,
    verbose=True,
    return_intermediate_steps=True,
)