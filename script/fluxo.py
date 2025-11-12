import os
from pathlib import Path
import sys
import json
from unstructured.partition.auto import partition
from unstructured.chunking import basic
from unstructured.documents.elements import Element
from pydantic import SecretStr
from unstructured.embed.openai import OpenAIEmbeddingEncoder, OpenAIEmbeddingConfig
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent  # sobe 1 nível (ajuste se precisar)

# ========================= unstructured_pipeline.py =========================
NAME_FILE = "Relatorio_Incidente_Ransomware_ACME_IR-2025-041.txt"
FILE_TO_PROCESS = PROJECT_ROOT / "data" / NAME_FILE

print(f"Processing file: {FILE_TO_PROCESS}")

# --- Particionamento ---
partitions = partition(filename=str(FILE_TO_PROCESS))
print(f"Number of partitions: {len(partitions)}")

# --- Chunking ---
chunker = basic.chunk_elements(elements=partitions)
print(f"Number of chunks created: {len(chunker)}")

# --- embedding openai ---
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    print("ERRO: defina OPENAI_API_KEY no ambiente antes de rodar.")
    sys.exit(1)

config = OpenAIEmbeddingConfig(api_key=SecretStr(api_key), model_name="text-embedding-3-small")
encoder = OpenAIEmbeddingEncoder(config=config)
embeddings = encoder.embed_documents(elements=chunker)
print(f"Number of embeddings created: {len(embeddings)}")

# NEW: normaliza embeddings + prepara registros (id, texto, vetor)
records = []
stem = Path(NAME_FILE).stem

def _element_text(el):
    txt = None
    if hasattr(el, "get_text"):
        try:
            txt = el.get_text()
        except Exception:
            txt = None
    if txt is None:
        txt = getattr(el, "text", None)
    return txt if txt is not None else str(el)

if embeddings and isinstance(embeddings[0], (list, tuple)):
    # Caso em que a lib retorna uma lista de vetores
    for i, vec in enumerate(embeddings):
        text = _element_text(chunker[i])
        records.append({
            "id": f"{stem}_{i}",
            "text": text,
            "embedding": list(map(float, vec)),
            "idx": i,
        })
else:
    # Caso em que a lib retorna Elements com .embedding
    for i, el in enumerate(embeddings):
        vec = getattr(el, "embedding", None) or getattr(el, "embeddings", None)
        if vec is None and getattr(el, "metadata", None):
            vec = el.metadata.get("embedding") or el.metadata.get("embeddings")
        text = _element_text(el)
        records.append({
            "id": f"{stem}_{i}",
            "text": text,
            "embedding": list(vec) if vec is not None else None,
            "idx": i,
        })

# salvar embeddings em disco formato JSONL
output_path = PROJECT_ROOT / "data" / f"{Path(NAME_FILE).stem}_embeddings.jsonl"
with output_path.open("w", encoding="utf-8") as f:
    for i, el in enumerate(embeddings):
        # tenta encontrar o vetor de embedding em vários locais possíveis
        emb_vector = None
        if hasattr(el, "embedding"):
            emb_vector = getattr(el, "embedding")
        elif getattr(el, "metadata", None) and isinstance(el.metadata, dict):
            emb_vector = el.metadata.get("embedding")

        # tenta extrair texto do elemento
        text = None
        if hasattr(el, "get_text"):
            try:
                text = el.get_text()
            except Exception:
                text = None
        if text is None:
            text = getattr(el, "text", None)
        if text is None:
            # fallback
            text = str(el)

        record = {
            "id": f"{Path(NAME_FILE).stem}_{i}",
            "text": text,
            "embedding": emb_vector,
        }
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

print(f"Embeddings saved to: {output_path}")

# ========================= Neo4j ingest =========================
# Usa variáveis de ambiente para não "acoplar" nada no código
NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

if not (NEO4J_URI and NEO4J_USERNAME and NEO4J_PASSWORD):
    print("NEO4J_* não configuradas; pulando ingestão no Neo4j.")
    sys.exit(0)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

def init_schema(session, dim: int | None):
    session.run("CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE")
    session.run("CREATE CONSTRAINT chunk_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.id IS UNIQUE")
    if dim:
        # Neo4j >= 5.11 – índice vetorial nativo
        session.run("""
            CREATE VECTOR INDEX chunk_embedding_index IF NOT EXISTS
            FOR (c:Chunk) ON (c.embedding)
            OPTIONS {indexConfig: {`vector.dimensions`: $dim, `vector.similarity_function`: 'cosine'}}
        """, {"dim": dim})

dim = len(records[0]["embedding"]) if records and records[0]["embedding"] else None

with driver.session(database=NEO4J_DATABASE) as session:
    init_schema(session, dim)
    doc_id = stem
    session.run(
        "MERGE (d:Document {id: $id}) "
        "SET d.name = $name, d.path = $path",
        {"id": doc_id, "name": NAME_FILE, "path": str(FILE_TO_PROCESS)},
    )

    # upsert dos chunks + relacionamentos
    for r in records:
        session.run("""
            MERGE (c:Chunk {id: $id})
            SET c.text = $text,
                c.embedding = $embedding,
                c.idx = $idx
            WITH c
            MATCH (d:Document {id: $doc_id})
            MERGE (d)-[:HAS_CHUNK]->(c)
        """, {"id": r["id"], "text": r["text"], "embedding": r["embedding"], "idx": r["idx"], "doc_id": doc_id})

        if r["idx"] > 0:
            prev_id = f"{doc_id}_{r['idx']-1}"
            session.run("""
                MATCH (c1:Chunk {id: $prev}), (c2:Chunk {id: $curr})
                MERGE (c1)-[:NEXT]->(c2)
            """, {"prev": prev_id, "curr": r["id"]})

driver.close()
print(f"Ingested {len(records)} chunks to Neo4j.")
