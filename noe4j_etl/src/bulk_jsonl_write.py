import os
import json
import logging
from neo4j import GraphDatabase

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger("etl-jsonl-to-neo4j")

# Diretório com arquivos JSONL processados
INPUT_DIR = "jsonl_processed"

# Configurações do Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

def ensure_constraints(driver):
    logger.info("Criando constraints se não existirem...")
    queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chunk) REQUIRE c.id IS UNIQUE"
    ]
    with driver.session(database=NEO4J_DATABASE) as session:
        for q in queries:
            session.run(q)

def insert_document_and_chunks(session, doc_id, filename, filetype, chunks):
    # Cria ou atualiza o nó Document
    session.run("""
        MERGE (d:Document {id: $doc_id})
        SET d.name = $filename,
            d.filetype = $filetype,
            d.source = $filename,
            d.created_at = COALESCE(d.created_at, timestamp())
    """, doc_id=doc_id, filename=filename, filetype=filetype)

    # Insere chunks e relacionamentos
    session.run("""
        UNWIND $batch AS row
        MERGE (c:Chunk {id: row.chunk_id})
        SET c.text = row.text,
            c.order = row.order,
            c.element_type = row.element_type,
            c.page_number = row.page_number
        WITH c, row
        MATCH (d:Document {id: $doc_id})
        MERGE (d)-[:HAS_CHUNK]->(c)
    """, batch=chunks, doc_id=doc_id)

    # Cria relacionamentos NEXT entre chunks consecutivos
    if len(chunks) > 1:
        session.run("""
            UNWIND $batch AS row
            MATCH (c:Chunk {id: row.chunk_id})
            WITH c, row
            ORDER BY row.order
            WITH collect(c) AS cs
            FOREACH (i IN range(0, size(cs)-2) |
                MERGE (cs[i])-[:NEXT]->(cs[i+1])
            )
        """, batch=chunks)

def process_jsonl_file(driver, path):
    logger.info(f"Processando arquivo: {path}")
    with open(path, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]

    if not records:
        logger.warning(f"Nenhum registro válido em {path}")
        return

    # Agrupa por doc_id
    docs = {}
    for rec in records:
        doc_id = rec["doc_id"]
        if doc_id not in docs:
            docs[doc_id] = {
                "filename": rec.get("filename"),
                "filetype": rec.get("filetype"),
                "chunks": []
            }
        docs[doc_id]["chunks"].append({
            "chunk_id": rec["chunk_id"],
            "text": rec["text"],
            "order": rec.get("order", 0),
            "element_type": rec.get("element_type"),
            "page_number": rec.get("page_number")
        })

    # Insere no Neo4j
    with driver.session(database=NEO4J_DATABASE) as session:
        for doc_id, data in docs.items():
            insert_document_and_chunks(session, doc_id, data["filename"], data["filetype"], data["chunks"])
    logger.info(f"Documento(s) inserido(s) do arquivo {path}")

def main():
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
        driver.verify_connectivity()
        logger.info("Conexão com Neo4j estabelecida.")
    except Exception as e:
        logger.error(f"Falha ao conectar ao Neo4j: {e}")
        return

    ensure_constraints(driver)

    if not os.path.isdir(INPUT_DIR):
        logger.error(f"Diretório não encontrado: {INPUT_DIR}")
        driver.close()
        return

    files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if f.endswith(".jsonl")]
    if not files:
        logger.warning(f"Nenhum arquivo .jsonl encontrado em {INPUT_DIR}")

    for path in files:
        process_jsonl_file(driver, path)

    driver.close()
    logger.info("ETL concluído.")

if __name__ == "__main__":
    main()