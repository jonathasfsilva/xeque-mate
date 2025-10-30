import os
from langchain_community.graphs import Neo4jGraph
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_community.vectorstores.neo4j_vector import Neo4jVector
from langchain_openai import OpenAIEmbeddings
# Assumindo que sua classe customizada está no mesmo local
from src.langchain_custom.graph_qa.cypher import GraphCypherQAChain 

# --- Variáveis de Ambiente ---
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Modelos de LLM (use os nomes dos modelos que preferir)
CTI_QA_MODEL = os.getenv("CTI_QA_MODEL", "gpt-4")
CTI_CYPHER_MODEL = os.getenv("CTI_CYPHER_MODEL", "gpt-4")

# Configuração para buscar exemplos de Cypher (Few-shot learning)
# Isso assume que você tem nós no Neo4j, ex: (:CtiQuestion {question: "...", cypher: "..."})
CTI_CYPHER_EXAMPLES_INDEX_NAME = os.getenv(
    "CTI_CYPHER_EXAMPLES_INDEX_NAME", "cti_question_index"
)
CTI_CYPHER_EXAMPLES_NODE_LABEL = os.getenv(
    "CTI_CYPHER_EXAMPLES_NODE_LABEL", "CtiQuestion"
)
CTI_CYPHER_EXAMPLES_TEXT_NODE_PROPERTY = os.getenv(
    "CTI_CYPHER_EXAMPLES_TEXT_NODE_PROPERTY", "question"
)

# --- Inicialização do Grafo ---
graph = Neo4jGraph(
    url=NEO4J_URI,
    username=NEO4J_USERNAME,
    password=NEO4J_PASSWORD,
)
# Atualiza o schema para o LLM (puxará ThreatActor, Victim, etc.)
graph.refresh_schema()

# --- Retriever para Exemplos de Cypher ---
# Esta parte busca exemplos de perguntas/respostas Cypher 
# que você armazenou no grafo para ajudar o LLM a gerar
# consultas melhores (few-shot retrieval).
try:
    cypher_example_index = Neo4jVector.from_existing_graph(
        embedding=OpenAIEmbeddings(),
        url=NEO4J_URI,
        username=NEO4J_USERNAME,
        password=NEO4J_PASSWORD,
        index_name=CTI_CYPHER_EXAMPLES_INDEX_NAME,
        node_label=CTI_CYPHER_EXAMPLES_NODE_LABEL,
        text_node_properties=[
            CTI_CYPHER_EXAMPLES_TEXT_NODE_PROPERTY,
        ],
        embedding_node_property="embedding",
    )
    cypher_example_retriever = cypher_example_index.as_retriever(search_kwargs={"k": 8})

except Exception as e:
    print(f"AVISO: Não foi possível carregar o índice de exemplos de Cypher '{CTI_CYPHER_EXAMPLES_INDEX_NAME}'. "
          f"A cadeia funcionará sem exemplos (zero-shot). Erro: {e}")
    cypher_example_retriever = None

# --- Prompt de Geração de Cypher (Atualizado) ---
cypher_generation_template = """
Task:
Gerar uma consulta Cypher para um banco de dados Neo4j.

Instructions:
Use apenas os tipos de relacionamento e propriedades fornecidos no schema.
Não use nenhum outro tipo de relacionamento ou propriedade que não seja fornecido.

Schema:
{schema}

Note:
Não inclua explicações ou desculpas em suas respostas.
Não responda a perguntas que peçam algo além de construir uma
declaração Cypher. Não inclua nenhum texto exceto a declaração
Cypher gerada. Certifique-se de que a direção do relacionamento
está correta em suas consultas. Certifique-se de usar alias
corretamente (ex: [r:TARGETED] em vez de [:TARGETED]). Não execute
consultas que adicionem ou excluam dados do banco de dados.

Example queries for this schema:
{example_queries}

Warning:
- Certifique-se de usar IS NULL ou IS NOT NULL ao analisar propriedades ausentes.
- Você nunca deve incluir a declaração "GROUP BY" em sua consulta.
- Certifique-se de usar alias em todas as declarações que seguem
  com a cláusula WITH (ex: WITH v as vitima, t.name as ator)
- Se precisar dividir números, certifique-se de filtrar o
  denominador para que não seja zero.

Valores de categoria de string:
As motivações do ThreatActor (motivation) são uma de: 'Financial', 'Espionage', 'Financial;Espionage'
As famílias de Ransomware (family) são uma de: 'Ransomware-as-a-Service', 'Ransomware', 'Crypto-Worm'
As moedas da Wallet (currency) são uma de: 'BTC', 'XMR'
As indústrias da Vítima (industry) são uma de: 'Saúde', 'Energia', 'Alimentícia', 'Governo', 'Tecnologia'

Se estiver filtrando por uma string, certifique-se de usar letras minúsculas
na propriedade e no valor de filtro, usando toLower().

A pergunta é:
{question}
"""

cypher_generation_prompt = PromptTemplate(
    input_variables=["schema", "example_queries", "question"],
    template=cypher_generation_template,
)

# --- Prompt de Geração de QA (Atualizado) ---
qa_generation_template = """Você é um assistente que pega os resultados de
uma consulta Cypher do Neo4j e forma uma resposta legível para humanos.
A seção de resultados da consulta contém os resultados de uma consulta
Cypher que foi gerada com base na pergunta do usuário em linguagem natural.
A informação fornecida é autoritativa; você deve sempre usá-la para
construir sua resposta sem duvidar ou corrigir usando conhecimento interno.
Faça a resposta soar como uma resposta à pergunta.

O usuário fez a seguinte pergunta:
{question}

Uma consulta Cypher foi executada e gerou estes resultados:
{context}

Se a informação fornecida estiver vazia, diga que não sabe a resposta.
Informação vazia se parece com isso: []

Se os resultados da consulta não estiverem vazios, você deve fornecer
uma resposta.
Quando nomes são fornecidos nos resultados da consulta (como nomes de
atores ou vítimas), retorne-os como uma lista clara.

Nunca diga que não tem a informação correta se houver dados nos
resultados da consulta. Você deve sempre presumir que quaisquer
resultados de consulta fornecidos são relevantes para responder à
pergunta. Construa sua resposta com base unicamente nos resultados
da consulta fornecidos.

Resposta Útil:
"""

qa_generation_prompt = PromptTemplate(
    input_variables=["context", "question"], template=qa_generation_template
)

# --- Construção da Cadeia Final ---
cypher_chain = GraphCypherQAChain.from_llm(
    cypher_llm=ChatOpenAI(model=CTI_CYPHER_MODEL, temperature=0),
    qa_llm=ChatOpenAI(model=CTI_QA_MODEL, temperature=0),
    # Passa o retriever de exemplos (pode ser None se falhou)
    cypher_example_retriever=cypher_example_retriever,
    # Propriedades para nunca retornar ao usuário
    node_properties_to_exclude=["embedding"], 
    graph=graph,
    verbose=True,
    qa_prompt=qa_generation_prompt,
    cypher_prompt=cypher_generation_prompt,
    validate_cypher=True,
    top_k=100,
)