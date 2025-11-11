# ingest_neo4j.py
# -*- coding: utf-8 -*-
"""
Ingestão no Neo4j a partir de JSONL no esquema:
{
  "id": "...",
  "doc_id": "...",
  "type": "paragraph" | "title" | ...,
  "text": "...",
  "n_tokens": 123,
  "metadata": {...},
  "embedding": [ ... floats ... ],              # opcional
  "embedding_model": "text-embedding-3-small"   # opcional
}

- Cria constraint única :Chunk(id)
- Detecta dimensão do embedding on-the-fly (na 1ª linha que tiver vetor) e cria índice vetorial
- Upsert em lotes; não apaga embeddings existentes quando a linha não possui embedding
"""

import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import ujson as json
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ClientError

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent  # sobe 1 nível (ajuste se precisar)
print(f"PROJECT_ROOT: {PROJECT_ROOT}")

# =========================
# CONFIG: edite aqui
# =========================
INPUT_JSONL     = PROJECT_ROOT / "embading" / "data" / "out_embedded_new.jsonl"  # saída do embed_openai.py
NEO4J_URI       = "neo4j+ssc://975d5047.databases.neo4j.io"
NEO4J_USER      = "neo4j"
NEO4J_PASSWORD  = "OuGdfpiDiOQ4qccf26hjDL5bVDduHUot-t18WNpB__0"
NEO4J_DATABASE  = "neo4j"

NODE_LABEL      = "Chunk"
UNIQUE_CONS     = "chunk_id_unique"
VECTOR_INDEX    = "chunk_embedding"
SIM_FUNCTION    = "cosine"          # cosine | euclidean | inner_product
BATCH_SIZE      = 400               # lote de MERGE
# =========================

# logs seguros no Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

def log(msg: str) -> None:
    try:
        print(msg)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, "encoding", "utf-8")
        sys.stdout.write(msg.encode(enc, errors="replace").decode(enc) + "\n")

def read_jsonl(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def get_driver() -> Driver:
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def ensure_constraint(driver: Driver):
    cypher = f"""
    CREATE CONSTRAINT {UNIQUE_CONS} IF NOT EXISTS
    FOR (c:{NODE_LABEL})
    REQUIRE c.id IS UNIQUE
    """
    with driver.session(database=NEO4J_DATABASE) as s:
        s.run(cypher).consume()
    log(f"[SCHEMA] constraint '{UNIQUE_CONS}' ok.")

def ensure_vector_index(driver: Driver, dim: int):
    """Cria índice vetorial com a dimensão detectada."""
    cypher = f"""
    CREATE VECTOR INDEX {VECTOR_INDEX} IF NOT EXISTS
    FOR (c:{NODE_LABEL}) ON (c.embedding)
    OPTIONS {{
      indexConfig: {{
        `vector.dimensions`: {dim},
        `vector.similarity_function`: '{SIM_FUNCTION}'
      }}
    }}
    """
    with driver.session(database=NEO4J_DATABASE) as s:
        try:
            s.run(cypher).consume()
            log(f"[SCHEMA] índice vetorial '{VECTOR_INDEX}' ok (dim={dim}, sim={SIM_FUNCTION}).")
        except ClientError as e:
            log(f"[AVISO] não foi possível (re)criar '{VECTOR_INDEX}' — possivelmente já existe com outra dimensão. {e}")

def upsert_batch(driver: Driver, rows: List[Dict]) -> int:
    """
    Faz MERGE por id. Se a linha trouxer 'embedding', atualiza; senão mantém o existente.
    """
    cypher = f"""
    UNWIND $rows AS row
    MERGE (c:{NODE_LABEL} {{id: row.id}})
      ON CREATE SET
        c.created_at = datetime(),
        c.doc_id = row.doc_id,
        c.type = row.type,
        c.types = row.types,
        c.text = row.text,
        c.n_tokens = row.n_tokens,
        c.source = row.source,
        c.metadata = row.metadata,
        c.embedding = row.embedding,
        c.embedding_model = row.embedding_model
      ON MATCH SET
        c.doc_id = coalesce(row.doc_id, c.doc_id),
        c.type = coalesce(row.type, c.type),
        c.types = coalesce(row.types, c.types),
        c.text = coalesce(row.text, c.text),
        c.n_tokens = coalesce(row.n_tokens, c.n_tokens),
        c.source = coalesce(row.source, c.source),
        c.metadata = coalesce(row.metadata, c.metadata),
        c.embedding = coalesce(row.embedding, c.embedding),
        c.embedding_model = coalesce(row.embedding_model, c.embedding_model),
        c.updated_at = datetime()
    RETURN count(*) AS upserts
    """
    with driver.session(database=NEO4J_DATABASE) as s:
        return s.run(cypher, rows=rows).single()["upserts"]

def main():
    in_path = Path(INPUT_JSONL)
    if not in_path.exists():
        log(f"[ERRO] arquivo não encontrado: {in_path}")
        sys.exit(1)

    driver = get_driver()
    ensure_constraint(driver)

    vector_index_created = False
    buffer: List[Dict] = []
    total, upserts = 0, 0

    for rec in read_jsonl(in_path):
        # novos campos do esquema: element_id, metadata.filename, etc.
        emb = rec.get("embedding")
        emb_model = rec.get("embedding_model")

        # cria o índice vetorial na 1ª vez que encontrar embedding
        if (not vector_index_created) and isinstance(emb, list) and emb:
            ensure_vector_index(driver, len(emb))
            vector_index_created = True

        # mapeamento adaptado ao novo esquema
        metadata = rec.get("metadata") or {}
        element_id = rec.get("element_id")
        rec_id = rec.get("id") or element_id or None

        filename = metadata.get("filename") or metadata.get("file") or metadata.get("file_name") or None
        # doc_id: prefira campo explícito, senão derive do filename (stem)
        doc_id = rec.get("doc_id") or metadata.get("doc_id") or (Path(filename).stem if filename else None)

        row = {
            "id": rec_id,
            "doc_id": doc_id,
            "type": rec.get("type"),
            "types": rec.get("types") if isinstance(rec.get("types"), list)
                     else [rec.get("type")] if rec.get("type") else None,
            "text": rec.get("text"),
            "n_tokens": rec.get("n_tokens"),
            "metadata": json.dumps(metadata or {}),
            "source": filename,
            "embedding": emb if (isinstance(emb, list) and emb) else None,
            "embedding_model": emb_model,
        }
        if not row["id"] or not row["text"]:
            # descarta entradas sem id (element_id) ou sem texto
            continue

        buffer.append(row)
        total += 1

        if len(buffer) >= BATCH_SIZE:
            upserts += upsert_batch(driver, buffer)
            log(f"[INFO] upsert: {upserts} (lidos: {total})")
            buffer = []

    if buffer:
        upserts += upsert_batch(driver, buffer)

    driver.close()
    log(f"OK: {upserts} nós upsertados de {total} linhas.")

if __name__ == "__main__":
    main()
