import os
import logging
from retry import retry
from neo4j import GraphDatabase

# Paths to CSV files containing data
RANSOMWARE_CSV_PATH = os.getenv("RANSOMWARE_CSV_PATH")
THREAT_ACTORS_CSV_PATH = os.getenv("THREAT_ACTORS_CSV_PATH")
VICTIM_CSV_PATH = os.getenv("VICTIM_CSV_PATH")
WALLET_CSV_PATH = os.getenv("WALLET_CSV_PATH")
ACTOR_OPERATES_CSV_PATH = os.getenv("ACTOR_OPERATES_CSV_PATH")
RANSOMWARE_TARGETED_CSV_PATH = os.getenv("RANSOMWARE_TARGETED_CSV_PATH")
RANSOMWARE_DEMANDS_CSV_PATH = os.getenv("RANSOMWARE_DEMANDS_CSV_PATH")
REPORTS_CSV_PATH = os.getenv("REPORTS_CSV_PATH")
REPORT_MENTIONS_ACTOR_CSV_PATH = os.getenv("REPORT_MENTIONS_ACTOR_CSV_PATH")
REPORT_MENTIONS_RANSOMWARE_CSV_PATH = os.getenv("REPORT_MENTIONS_RANSOMWARE_CSV_PATH")

# Neo4j config
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")

# Configure the logging module
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


LOGGER = logging.getLogger(__name__)

NODES = ["Ransomware", "ThreatActor", "Visit", "Wallet", "Report"]


def _set_uniqueness_constraints(tx, node):
    query = f"""CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node})
        REQUIRE n.id IS UNIQUE;"""
    _ = tx.run(query, {})

@retry(tries=100, delay=10)
def load_cti_graph_from_csv() -> None:
    """
    Carrega dados CSV estruturados de CTI (modelo de ransomware)
    para o Neo4j.
    """

    try:
        driver: Driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
        driver.verify_connectivity()
        LOGGER.info("Conexão com Neo4j estabelecida.")
    except Exception as e:
        LOGGER.error(f"Falha ao conectar com Neo4j: {e}")
        return

    LOGGER.info("Setting uniqueness constraints on nodes")
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            for node in NODES:
                session.execute_write(_set_uniqueness_constraints, node)
    except Exception as e:
        LOGGER.error(f"Falha ao criar constraints: {e}")
        driver.close()
        return

    # --- Carregar Nodes ---

    LOGGER.info("Loading ThreatActor nodes")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{THREAT_ACTORS_CSV_PATH}' AS row
        MERGE (t:ThreatActor {{id: row.id,
                               name: row.name,
                               origin: row.origin,
                               motivation: row.motivation}});
        """
        _ = session.run(query, {})

    LOGGER.info("Loading Ransomware nodes")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{RANSOMWARE_CSV_PATH}' AS row
        MERGE (r:Ransomware {{id: row.id,
                               name: row.name,
                               family: row.family}});
        """
        _ = session.run(query, {})

    LOGGER.info("Loading Victim nodes")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{VICTIM_CSV_PATH}' AS row
        MERGE (v:Victim {{id: row.id,
                               name: row.name,
                               industry: row.industry,
                               country: row.country}});
        """
        _ = session.run(query, {})

    LOGGER.info("Loading Wallet nodes")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{WALLET_CSV_PATH}' AS row
        MERGE (w:Wallet {{id: row.id,
                               currency: row.currency,
                               ransom_amount_usd: toFloat(row.ransom_amount_usd)
                               }});
        """
        _ = session.run(query, {})

    LOGGER.info("Loading Report nodes")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{REPORTS_CSV_PATH}' AS row
        MERGE (r:Report {{id: row.id,
                            title: row.title,
                            text: row.text,
                            source: row.source
                            }});
        """
        _ = session.run(query, {})

    # --- Carregar Relacionamentos ---

    LOGGER.info("Loading 'OPERATES' relationships")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{ACTOR_OPERATES_CSV_PATH}' AS row
        MATCH (source:ThreatActor {{id: trim(row.FROM_ID)}})
        MATCH (target:Ransomware {{id: trim(row.TO_ID)}})
        MERGE (source)-[r:OPERATES]->(target)
        """
        _ = session.run(query, {})

    LOGGER.info("Loading 'TARGETED' relationships")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{RANSOMWARE_TARGETED_CSV_PATH}' AS row
        MATCH (source:Ransomware {{id: trim(row.FROM_ID)}})
        MATCH (target:Victim {{id: trim(row.TO_ID)}})
        MERGE (source)-[r:TARGETED {{date_of_attack: row.date_of_attack}}]->(target)
        """
        _ = session.run(query, {})

    LOGGER.info("Loading 'DEMANDS_PAYMENT_TO' relationships")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{RANSOMWARE_DEMANDS_CSV_PATH}' AS row
        MATCH (source:Ransomware {{id: trim(row.FROM_ID)}})
        MATCH (target:Wallet {{id: trim(row.TO_ID)}})
        MERGE (source)-[r:DEMANDS_PAYMENT_TO]->(target)
        """
        _ = session.run(query, {})
    
    LOGGER.info("Loading 'MENTIONS' relationships (Report to ThreatActor)")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{REPORT_MENTIONS_ACTOR_CSV_PATH}' AS row
        MATCH (source:Report {{id: trim(row.FROM_ID)}})
        MATCH (target:ThreatActor {{id: trim(row.TO_ID)}})
        MERGE (source)-[r:MENTIONS]->(target)
        """
        _ = session.run(query, {})

    LOGGER.info("Loading 'MENTIONS' relationships (Report to Ransomware)")
    with driver.session(database=NEO4J_DATABASE) as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{REPORT_MENTIONS_RANSOMWARE_CSV_PATH}' AS row
        MATCH (source:Report {{id: trim(row.FROM_ID)}})
        MATCH (target:Ransomware {{id: trim(row.TO_ID)}})
        MERGE (source)-[r:MENTIONS]->(target)
        """
        _ = session.run(query, {})

    LOGGER.info("Carregamento do grafo de CTI concluído.")
    driver.close()


if __name__ == "__main__":
    load_cti_graph_from_csv()