import os, glob, uuid, math
from typing import List, Dict, Any, Iterable
from dotenv import load_dotenv
from tqdm import tqdm
from neo4j import GraphDatabase
import ujson as json

# OpenAI SDK v1
from openai import OpenAI

# tokenização para chunk
import tiktoken

load_dotenv()

OPENAI_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBED_DIM    = int(os.getenv("EMBED_DIM", "1536"))
MAX_TOK      = int(os.getenv("CHUNK_MAX_TOKENS", "400"))
OVERLAP_TOK  = int(os.getenv("CHUNK_OVERLAP_TOKENS", "40"))

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PWD  = os.getenv("NEO4J_PASSWORD")
NEO4J_DB   = os.getenv("NEO4J_DATABASE", "neo4j")

client = OpenAI()

def load_unstructured_records(path_pattern: str) -> Iterable[Dict[str, Any]]:
    for fp in glob.glob(path_pattern):
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
        yield {"file": os.path.basename(fp), "data": data}

def extract_blocks(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Aceita formatos comuns do unstructured.
    Retorna lista de {text, type, metadata}
    """
    blocks = []
    data = record["data"]
    if isinstance(data, dict) and "elements" in data:
        data = data["elements"]
    if not isinstance(data, list):
        return blocks

    for el in data:
        text = el.get("text") or el.get("content") or ""
        if not text.strip():
            continue
        etype = el.get("type") or el.get("category") or el.get("element_type") or "paragraph"
        meta = el.get("metadata") or {}
        blocks.append({
            "text": text,
            "type": etype,
            "metadata": meta
        })
    return blocks

def chunk_blocks(blocks: List[Dict[str, Any]], max_tok=MAX_TOK, overlap=OVERLAP_TOK) -> List[Dict[str, Any]]:
    enc = tiktoken.get_encoding("cl100k_base")
    chunks = []
    buf, buf_types, buf_meta = [], [], []

    def flush():
        if not buf:
            return
        text = "\n".join(buf).strip()
        if text:
            chunks.append({
                "text": text,
                "types": list(set(buf_types)),
                "metadata_merged": merge_meta(buf_meta)
            })

    cur_tokens = 0
    for b in blocks:
        t = b["text"].strip()
        toks = enc.encode(t)
        tlen = len(toks)

        if cur_tokens + tlen <= max_tok:
            buf.append(t)
            buf_types.append(b["type"])
            buf_meta.append(b["metadata"])
            cur_tokens += tlen
        else:
            flush()
            # overlap
            if overlap > 0 and chunks:
                # recomeça com parte final do último chunk (na prática, ignoramos por simplicidade)
                pass
            buf, buf_types, buf_meta = [t], [b["type"]], [b["metadata"]]
            cur_tokens = tlen
    flush()
    return chunks

def merge_meta(list_of_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    out = {}
    for m in list_of_meta:
        if not isinstance(m, dict):
            continue
        for k, v in m.items():
            if k not in out:
                out[k] = v
    return out

def embed_texts(texts: List[str]) -> List[List[float]]:
    # batch embeddings
    resp = client.embeddings.create(
        model=OPENAI_MODEL,
        input=texts
    )
    return [d.embedding for d in resp.data]

def upsert_chunks(chunks: List[Dict[str, Any]], source_file: str):
    rows = []
    for i, ch in enumerate(chunks):
        rows.append({
            "id": f"{source_file}:{i}:{uuid.uuid4().hex[:8]}",
            "text": ch["text"],
            "source": source_file,
            "types": ch.get("types", []),
            "metadata": ch.get("metadata_merged", {})
        })
    # embeddings em lote
    embeddings = embed_texts([r["text"] for r in rows])
    for r, emb in zip(rows, embeddings):
        r["embedding"] = emb

    cypher = """
    UNWIND $rows AS row
    MERGE (c:Chunk {id: row.id})
      ON CREATE SET
        c.text = row.text,
        c.source = row.source,
        c.types = row.types,
        c.metadata = row.metadata,
        c.created_at = datetime(),
        c.embedding = row.embedding
      ON MATCH SET
        c.text = row.text,
        c.types = row.types,
        c.metadata = row.metadata,
        c.embedding = row.embedding
    RETURN count(*) as upserts
    """

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PWD))
    with driver.session(database=NEO4J_DB) as session:
        result = session.run(cypher, rows=rows).single()
        print(f"Neo4j upserts: {result['upserts']}")
    driver.close()

def main():
    pattern = os.path.join(os.path.dirname(__file__), "..", "data", "unstructured_out", "*.json")
    total_chunks = 0
    for rec in load_unstructured_records(pattern):
        blocks = extract_blocks(rec)
        chunks = chunk_blocks(blocks)
        if not chunks:
            continue
        upsert_chunks(chunks, rec["file"])
        total_chunks += len(chunks)
    print(f"Finalizado. Chunks inseridos: {total_chunks}")

if __name__ == "__main__":
    main()
