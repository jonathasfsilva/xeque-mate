import os
from typing import List, Tuple
from dotenv import load_dotenv
from neo4j import GraphDatabase
from openai import OpenAI

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PWD  = os.getenv("NEO4J_PASSWORD")
NEO4J_DB   = os.getenv("NEO4J_DATABASE", "neo4j")

EMBED_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
CHAT_MODEL  = os.getenv("CHAT_MODEL", "gpt-4o-mini")
TOP_K = int(os.getenv("TOP_K", "5"))

client = OpenAI()

def embed(q: str) -> List[float]:
    return client.embeddings.create(model=EMBED_MODEL, input=q).data[0].embedding

def knn(embedding: List[float], k: int = TOP_K) -> List[Tuple[str, dict, float]]:
    cypher = """
    CALL db.index.vector.queryNodes('chunk_embedding', $k, $embedding)
    YIELD node, score
    RETURN node.text AS text, node.metadata AS metadata, score
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PWD))
    with driver.session(database=NEO4J_DB) as session:
        rows = session.run(cypher, k=k, embedding=embedding).data()
    driver.close()
    return [(r["text"], r["metadata"], r["score"]) for r in rows]

def build_prompt(question: str, contexts: List[str]) -> str:
    head = (
        "Você é um assistente para CTI focado em ransomware.\n"
        "Use APENAS o contexto abaixo. Seja objetivo e cite trechos relevantes entre aspas quando útil.\n\n"
        "### Contexto\n"
    )
    ctx = "\n\n---\n\n".join(contexts[:TOP_K])
    tail = f"\n\n### Pergunta\n{question}\n\n### Resposta:"
    return head + ctx + tail

def answer(question: str) -> str:
    emb = embed(question)
    hits = knn(emb, TOP_K)
    contexts = [t for (t, m, s) in hits]
    prompt = build_prompt(question, contexts)

    resp = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return resp.choices[0].message.content

if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) or "Quais IOCs são mencionados?"
    print(answer(q))
