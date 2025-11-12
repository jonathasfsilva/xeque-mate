import os
from pathlib import Path
from typing import List, Dict, Optional
from neo4j import GraphDatabase


def _get_env():
    return {
        "uri": os.environ.get("NEO4J_URI"),
        "username": os.environ.get("NEO4J_USERNAME"),
        "password": os.environ.get("NEO4J_PASSWORD"),
        "database": os.environ.get("NEO4J_DATABASE", "neo4j"),
    }


def init_schema(session, dim: Optional[int]):
    session.run("CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE")
    session.run("CREATE CONSTRAINT chunk_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.id IS UNIQUE")
    if dim:
        session.run(
            """
            CREATE VECTOR INDEX chunk_embedding_index IF NOT EXISTS
            FOR (c:Chunk) ON (c.embedding)
            OPTIONS {indexConfig: {`vector.dimensions`: $dim, `vector.similarity_function`: 'cosine'}}
            """,
            {"dim": dim},
        )


def ingest_records(records: List[Dict], name_file: str, file_path: Path) -> int:
    cfg = _get_env()
    if not (cfg["uri"] and cfg["username"] and cfg["password"]):
        print("NEO4J_* não configuradas; pulando ingestão no Neo4j.")
        return 0

    driver = GraphDatabase.driver(cfg["uri"], auth=(cfg["username"], cfg["password"]))
    dim = len(records[0]["embedding"]) if records and records[0].get("embedding") else None
    stem = Path(name_file).stem

    with driver.session(database=cfg["database"]) as session:
        init_schema(session, dim)
        doc_id = stem
        session.run(
            "MERGE (d:Document {id: $id}) "
            "SET d.name = $name, d.path = $path",
            {"id": doc_id, "name": name_file, "path": str(file_path)},
        )

        for r in records:
            session.run(
                """
                MERGE (c:Chunk {id: $id})
                SET c.text = $text,
                    c.embedding = $embedding,
                    c.idx = $idx
                WITH c
                MATCH (d:Document {id: $doc_id})
                MERGE (d)-[:HAS_CHUNK]->(c)
                """,
                {
                    "id": r["id"],
                    "text": r.get("text"),
                    "embedding": r.get("embedding"),
                    "idx": r.get("idx"),
                    "doc_id": doc_id,
                },
            )

            if r.get("idx", 0) > 0:
                prev_id = f"{doc_id}_{r['idx']-1}"
                session.run(
                    """
                    MATCH (c1:Chunk {id: $prev}), (c2:Chunk {id: $curr})
                    MERGE (c1)-[:NEXT]->(c2)
                    """,
                    {"prev": prev_id, "curr": r["id"]},
                )

    driver.close()
    print(f"Ingested {len(records)} chunks to Neo4j.")
    return len(records)