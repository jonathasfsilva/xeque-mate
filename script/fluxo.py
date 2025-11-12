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
