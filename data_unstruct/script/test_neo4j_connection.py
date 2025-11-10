# test_neo4j_connection.py
from __future__ import annotations
import os
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from dotenv import load_dotenv

load_dotenv()

def try_connect(uri: str) -> bool:
    user = os.getenv("NEO4J_USERNAME", "neo4j")
    pwd  = os.getenv("NEO4J_PASSWORD")
    db   = os.getenv("NEO4J_DATABASE", "neo4j")
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    try:
        with driver.session(database=db) as s:
            ok = s.run("RETURN 1 AS ok").single()["ok"] == 1
            print(f"[OK] Conectado via {uri} | db={db}")
            return ok
    finally:
        driver.close()

uri_env = os.getenv("NEO4J_URI", "neo4j+s://975d5047.databases.neo4j.io")
print(f"Tentando conectar em: {uri_env}")

try:
    try_connect(uri_env)
except ServiceUnavailable as e:
    msg = str(e)
    if "SSLCertVerificationError" in msg or "Failed to establish encrypted connection" in msg:
        # fallback para aceitar cadeia 'self-signed'
        if uri_env.startswith("neo4j+s://"):
            uri_ssc = uri_env.replace("neo4j+s://", "neo4j+ssc://")
        elif uri_env.startswith("bolt+s://"):
            uri_ssc = uri_env.replace("bolt+s://", "bolt+ssc://")
        else:
            uri_ssc = "neo4j+ssc://" + uri_env.split("://")[-1]
        print(f"[WARN] TLS verification falhou. Tentando fallback: {uri_ssc}")
        try_connect(uri_ssc)
    else:
        raise
