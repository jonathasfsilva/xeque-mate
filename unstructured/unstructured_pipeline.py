# unstructured_pipeline.py
# -*- coding: utf-8 -*-
"""
Pipeline Unstructured: segmentação, classificação, metadados, normalização/limpeza.
Compatível com: PDF, DOC/DOCX, HTML/HTM, EML/MSG, TXT/MD, etc.
Saída: JSONL com chunks normalizados e metadados prontos para embeddings/RAG.

Exemplo:
    python unstructured_pipeline.py --input ./data/raw --output ./data/out.jsonl
"""

import argparse
import os
import re
import uuid
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import ujson as json

try:
    from unstructured.partition.auto import partition
except Exception as e:
    raise SystemExit(
        "Erro importando 'unstructured'. Instale com:\n"
        '  pip install "unstructured[all-docs]"'
    ) from e

# Tokenização para chunk
try:
    import tiktoken
    _ENC = tiktoken.get_encoding("cl100k_base")
    def count_tokens(text: str) -> int:
        return len(_ENC.encode(text))
    def split_by_tokens(text: str, max_tokens: int, overlap: int) -> List[str]:
        toks = _ENC.encode(text)
        if len(toks) <= max_tokens:
            return [text]
        chunks = []
        start = 0
        while start < len(toks):
            end = min(start + max_tokens, len(toks))
            piece = _ENC.decode(toks[start:end])
            chunks.append(piece)
            if end == len(toks):
                break
            start = end - overlap if overlap > 0 else end
            if start < 0:
                start = 0
        return chunks
except Exception:
    # Fallback simples por caracteres
    def count_tokens(text: str) -> int:
        return max(1, len(text) // 4)  # heurística grosseira
    def split_by_tokens(text: str, max_tokens: int, overlap: int) -> List[str]:
        max_chars = max_tokens * 4
        if len(text) <= max_chars:
            return [text]
        chunks = []
        start = 0
        while start < len(text):
            end = min(start + max_chars, len(text))
            piece = text[start:end]
            chunks.append(piece)
            if end == len(text):
                break
            start = max(0, end - overlap * 4)
        return chunks


# --- Normalização / limpeza ---------------------------------------------------

_ZERO_WIDTH = "[\u200B-\u200F\uFEFF]"
_CTRL = r"[\x00-\x08\x0B-\x0C\x0E-\x1F]"

def normalize_text(text: str) -> str:
    """Normaliza e limpa o texto mantendo parágrafos."""
    if not text:
        return ""
    # Normaliza quebras e BOM/zero-width
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(_ZERO_WIDTH, "", t)
    t = re.sub(_CTRL, " ", t)

    # Des-hifenização comum: quebra de linha com hífen
    t = re.sub(r"(\w)-\n(\w)", r"\1\2", t)
    # Colapsa espaços
    t = re.sub(r"[ \t\f\v]+", " ", t)
    # Mantém no máx 2 quebras seguidas (delimita parágrafos)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


# --- Classificação ------------------------------------------------------------

CATEGORY_MAP = {
    "Title": "title",
    "NarrativeText": "paragraph",
    "ListItem": "list",
    "Table": "table",
    "Header": "header",
    "Footer": "footer",
    "FigureCaption": "caption",
    "Image": "image",
    "CodeSnippet": "code",
    "PageBreak": "page_break",
    "UncategorizedText": "paragraph",
    "CompositeElement": "paragraph",
}

def classify_category(cat: Optional[str]) -> str:
    if not cat:
        return "paragraph"
    return CATEGORY_MAP.get(cat, cat.lower())


# --- Segmentação (via unstructured + re-chunk) --------------------------------

def segment_file(path: Path) -> Tuple[List, Dict]:
    """Usa o unstructured para obter elementos e metadados do documento."""
    elements = partition(filename=str(path))  # auto-detecta tipo
    # Metadado geral do doc (nem sempre está preenchido; mantemos filename/ext)
    doc_meta = {
        "doc_id": path.stem,
        "filename": path.name,
        "suffix": path.suffix.lower(),
        "parent": str(path.parent),
    }
    return elements, doc_meta


def element_text(el) -> str:
    """Extrai texto do elemento, caindo para str(el) se necessário."""
    # Em versões recentes: el.text; algumas têm .to_dict()["text"]
    txt = getattr(el, "text", None)
    if not txt:
        try:
            d = el.to_dict()
            txt = d.get("text") or ""
        except Exception:
            txt = str(el) or ""
    return txt


def element_metadata(el) -> Dict:
    """Extrai metadados estruturados do elemento."""
    md = {}
    try:
        md_obj = getattr(el, "metadata", None)
        if md_obj and hasattr(md_obj, "to_dict"):
            md = md_obj.to_dict()
    except Exception:
        pass
    # Filtragem leve de campos grandes/voláteis
    for k in ("coordinates", "languages", "data_source"):
        if k in md and md[k] is None:
            md.pop(k, None)
    return md


# --- Transformação em chunks ---------------------------------------------------

def elements_to_chunks(
    elements: List,
    doc_meta: Dict,
    max_tokens: int,
    overlap: int,
) -> Iterable[Dict]:
    for idx, el in enumerate(elements):
        raw_text = element_text(el)
        if not raw_text or not raw_text.strip():
            continue

        norm_text = normalize_text(raw_text)
        if not norm_text:
            continue

        cat = classify_category(getattr(el, "category", None))
        emd = element_metadata(el)

        # Re-chunk para elementos longos
        parts = split_by_tokens(norm_text, max_tokens, overlap)
        for part_idx, part in enumerate(parts):
            yield {
                "id": f"{doc_meta['filename']}::{idx:05d}:{part_idx:03d}:{uuid.uuid4().hex[:6]}",
                "doc_id": doc_meta["doc_id"],
                "source_path": os.path.join(doc_meta["parent"], doc_meta["filename"]),
                "type": cat,                     # Classificação do conteúdo
                "text": part,                    # Texto normalizado/limpo
                "n_tokens": count_tokens(part),  # aprox. para tuning de chunk
                "metadata": {
                    **emd,
                    **doc_meta,
                    "element_index": idx,
                    "chunk_index": part_idx,
                },
            }


# --- Coleta de arquivos -------------------------------------------------------

VALID_SUFFIXES = {
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".html", ".htm", ".xml",
    ".eml", ".msg",
    ".txt", ".md", ".rtf",
}

def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in VALID_SUFFIXES:
            yield p


# --- CLI ----------------------------------------------------------------------

def run_pipeline(input_dir: Path, output_path: Path, max_tokens: int, overlap: int) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_docs = 0
    total_chunks = 0

    with output_path.open("w", encoding="utf-8") as out:
        for file_path in iter_files(input_dir):
            total_docs += 1
            try:
                elements, doc_meta = segment_file(file_path)
            except Exception as e:
                print(f"[ERRO] {file_path}: {e}")
                continue

            for chunk in elements_to_chunks(elements, doc_meta, max_tokens, overlap):
                out.write(json.dumps(chunk, ensure_ascii=False) + "\n")
                total_chunks += 1

    print(f"OK: {total_docs} documentos processados → {total_chunks} chunks em {output_path}")

def main():
    ap = argparse.ArgumentParser(description="Pipeline Unstructured (seg/class/meta/normalize).")
    ap.add_argument("--input", type=Path, required=True, help="Pasta com documentos de entrada.")
    ap.add_argument("--output", type=Path, required=True, help="Arquivo .jsonl de saída.")
    ap.add_argument("--max-tokens", type=int, default=400, help="Tamanho máximo do chunk.")
    ap.add_argument("--overlap", type=int, default=40, help="Overlap em tokens entre chunks.")
    args = ap.parse_args()

    run_pipeline(args.input, args.output, args.max_tokens, args.overlap)

if __name__ == "__main__":
    main()
