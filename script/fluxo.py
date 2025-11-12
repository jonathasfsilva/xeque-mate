import sys
from pathlib import Path
from unstructured.partition.auto import partition
from unstructured.chunking import basic
from unstructured.documents.elements import Element
from unstructured.embed.openai import OpenAIEmbeddingEncoder as emb_openai
import os
from pydantic import SecretStr
from unstructured.embed.openai import OpenAIEmbeddingEncoder, OpenAIEmbeddingConfig
from dotenv import load_dotenv

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
print(embeddings)

# salvar embeddings em disco formato JSONL
output_path = PROJECT_ROOT / "data" / f"{Path(NAME_FILE).stem}_embeddings.jsonl"
with output_path.open("w", encoding="utf-8") as f:
    for emb in embeddings:
        f.write(emb.to_json() + "\n")
print(f"Embeddings saved to: {output_path}")
