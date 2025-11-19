import os
import json
import uuid
import re
from pathlib import Path

# ------------------------------
#   REGEX E FUNÇÕES UTILITÁRIAS
# ------------------------------

RE_MULTISPACE = re.compile(r"\s+")
RE_HEADER_FOOTER = re.compile(
    r"(NIST SP 800-61r3|This publication is available free of charge).*",
    re.IGNORECASE
)

def stable_uuid(s: str) -> str:
    """Gera UUID estável baseado no conteúdo."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, s))


def clean_text(text: str) -> str:
    """Remove cabeçalhos/rodapés e normaliza espaços."""
    text = RE_HEADER_FOOTER.sub("", text)
    text = RE_MULTISPACE.sub(" ", text)
    return text.strip()


# ------------------------------
#   PROCESSADOR DE UM ARQUIVO
# ------------------------------

def process_jsonl_file(path: str, output_dir: Path):
    """
    Processa um único arquivo .jsonl e salva no diretório informado.
    """
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
            page_number = metadata.get("page_number")
            element_type = rec.get("type", "Unknown")

            # IDs determinísticos
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

    # Criar saída
    os.makedirs(output_dir, exist_ok=True)
    out_path = output_dir / filename

    with open(out_path, "w", encoding="utf-8") as out_f:
        for rec in processed_records:
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"[✔] Processado: {path} → {out_path}")

def preprocess_jsonl(raw_dir: str | Path, processed_dir: str | Path):
    """
    Processa todos os JSONL presentes em `raw_dir`
    e salva JSONL limpos em `processed_dir`.

    Parâmetros:
        raw_dir (str | Path): pasta contendo arquivos .jsonl brutos
        processed_dir (str | Path): pasta onde salvar arquivos processados
    """
    raw_dir = Path(raw_dir)
    processed_dir = Path(processed_dir)

    os.makedirs(processed_dir, exist_ok=True)

    files = [f for f in raw_dir.iterdir() if f.suffix == ".jsonl"]

    if not files:
        print(f"[!] Nenhum JSONL encontrado em: {raw_dir}")
        return

    print(f"\n=== Iniciando preprocessamento JSONL ===")
    print(f"Entrada : {raw_dir}")
    print(f"Saída   : {processed_dir}\n")

    for file_path in files:
        process_jsonl_file(file_path, processed_dir)

    print(f"\n=== Finalizado com sucesso ===")
    print(f"Arquivos pré-processados salvos em: {processed_dir}\n")
