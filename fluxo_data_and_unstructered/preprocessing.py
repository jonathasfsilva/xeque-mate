import os
import json
import uuid
import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

print(f"Script directory: {SCRIPT_DIR}")

RAW_DIR = SCRIPT_DIR / "coletor_data" / "data" / "chunks_jsonl"
PROCESSED_DIR = SCRIPT_DIR / "jsonl_processed"

print(f"Raw data directory: {RAW_DIR}")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# Regex para limpeza
RE_MULTISPACE = re.compile(r"\s+")
RE_HEADER_FOOTER = re.compile(r"(NIST SP 800-61r3|This publication is available free of charge).*", re.IGNORECASE)

def stable_uuid(s: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, s))

def clean_text(text: str) -> str:
    # Remove cabeçalhos/rodapés repetidos
    text = RE_HEADER_FOOTER.sub("", text)
    # Normaliza espaços
    text = RE_MULTISPACE.sub(" ", text)
    return text.strip()

def process_jsonl_file(path: str):
    filename = os.path.basename(path)
    processed_records = []
    with open(path, "r", encoding="utf-8") as f:
        for order, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            text = clean_text(rec.get("text", ""))
            if not text:
                continue

            metadata = rec.get("metadata", {})
            filetype = metadata.get("filetype", "")
            page_number = metadata.get("page_number", None)
            element_type = rec.get("type", "Unknown")

            doc_id = stable_uuid(f"document::{metadata.get('filename', filename)}")
            chunk_id = stable_uuid(f"{doc_id}:{order}:{len(text)}")

            processed_records.append({
                "chunk_id": chunk_id,
                "doc_id": doc_id,
                "filename": metadata.get("filename", filename),
                "filetype": filetype,
                "page_number": page_number,
                "element_type": element_type,
                "text": text,
                "order": order
            })

    # Salva arquivo processado
    out_path = os.path.join(PROCESSED_DIR, filename)
    with open(out_path, "w", encoding="utf-8") as out_f:
        for rec in processed_records:
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def main():
    files = [os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if f.endswith(".jsonl")]
    for path in files:
        process_jsonl_file(path)
    print(f"Processamento concluído. Arquivos salvos em {PROCESSED_DIR}")

if __name__ == "__main__":
    main()