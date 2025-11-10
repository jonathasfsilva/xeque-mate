import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PWD  = os.getenv("NEO4J_PASSWORD")
DB   = os.getenv("NEO4J_DATABASE", "neo4j")
DIM  = int(os.getenv("EMBED_DIM", "1536"))

CYPHER = f"""
CREATE VECTOR INDEX chunk_embedding IF NOT EXISTS
FOR (c:Chunk) ON (c.embedding)
OPTIONS {{
  indexConfig: {{
    `vector.dimensions`: {DIM},
    `vector.similarity_function`: 'cosine'
  }}
}};
CREATE INDEX chunk_id IF NOT EXISTS FOR (c:Chunk) ON (c.id);
"""

def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    with driver.session(database=DB) as session:
        session.run(CYPHER)
    driver.close()
    print(f"OK: índice vetorial 'chunk_embedding' ({DIM} dims) e índice por id criados.")

if __name__ == "__main__":
    main()
