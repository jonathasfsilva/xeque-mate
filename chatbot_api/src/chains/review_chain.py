import os
from langchain_community.vectorstores import Neo4jVector
from langchain_openai import OpenAIEmbeddings
from langchain.chains import RetrievalQA
from langchain_openai import ChatOpenAI
from langchain.prompts import (
    PromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
    ChatPromptTemplate,
)

CTI_QA_MODEL = os.getenv("CTI_QA_MODEL", "gpt-4")

# --- Configuração do Índice Vetorial ---
# Ele buscará no nó 'Report' que acabamos de carregar
reports_index = Neo4jVector.from_existing_graph(
    embedding=OpenAIEmbeddings(),
    url=os.getenv("NEO4J_URI"),
    username=os.getenv("NEO4J_USERNAME"),
    password=os.getenv("NEO4J_PASSWORD"),
    index_name="cti_reports",  
    node_label="Report",      
    text_node_properties=[
        "title",
        "text",
        "source"
    ],
    embedding_node_property="embedding", # Onde o embedding será salvo
)


reports_template = """Você é um assistente de CTI. Seu trabalho é usar
relatórios de inteligência e boletins de ameaças para responder
perguntas sobre o 'modus operandi' (TTPs), motivações ou perfis de
atores de ameaça. Use o seguinte contexto para responder as
perguntas. Seja o mais detalhado possível, mas não invente
informações que não estejam no contexto. Se você não sabe a
resposta, diga que não sabe.
{context}
"""

reports_system_prompt = SystemMessagePromptTemplate(
    prompt=PromptTemplate(input_variables=["context"], template=reports_template)
)

reports_human_prompt = HumanMessagePromptTemplate(
    prompt=PromptTemplate(input_variables=["question"], template="{question}")
)
messages = [reports_system_prompt, reports_human_prompt]

reports_prompt = ChatPromptTemplate(
    input_variables=["context", "question"], messages=messages
)

# --- Montagem da Cadeia ---
cti_reports_vector_chain = RetrievalQA.from_chain_type(
    llm=ChatOpenAI(model=CTI_QA_MODEL, temperature=0),
    chain_type="stuff",
    retriever=reports_index.as_retriever(k=5), # Buscar 5 relatórios
)
cti_reports_vector_chain.combine_documents_chain.llm_chain.prompt = reports_prompt
