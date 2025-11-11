# unstructured_pipeline.py
# -*- coding: utf-8 -*-
"""
Pipeline Unstructured: Segmentação, Classificação, Metadados, Normalização/Limpeza
Compatível com: PDF, DOC/DOCX, PPT/PPTX, HTML/HTM, EML/MSG, TXT/MD, RTF
Saída: JSONL com chunks normalizados e metadados (pronto para embeddings/RAG).

Dependências mínimas (exemplos):
    pip install "unstructured[all-docs]>=0.12.0" tiktoken>=0.7.0 ftfy>=6.2.0 ujson>=5.10.0
Execução:
    python unstructured_pipeline.py
"""

import os
import re
import uuid
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import ujson as json

SCRIPT_DIR = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_DIR.parent  # sobe 1 nível (ajuste se precisar)
print(f"PROJECT_ROOT: {PROJECT_ROOT}")

# =========================
# CONFIG: edite aqui
# =========================
INPUT_PATH  = PROJECT_ROOT / "Relatorio_Incidente_Ransomware_ACME_IR-2025-041.txt"
OUTPUT_PATH = PROJECT_ROOT / "data" / "unstruct-output" / "out.jsonl"
MAX_TOKENS  = 400   # tamanho máximo do chunk (tokens)
OVERLAP     = 40    # overlap em tokens entre chunks
PREVIEW     = True  # imprime amostra do primeiro elemento de cada doc
# =========================

# --- Unstructured (segmentação) ---
try:
    from unstructured.partition.auto import partition
except Exception as e:
    raise SystemExit(
        "Erro importando 'unstructured'. Instale com:\n"
        '  pip install "unstructured[all-docs]"'
    ) from e

# --- Tokenização (para chunk) ---
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
    # Fallback por caracteres (heurístico, caso tiktoken não esteja disponível)
    def count_tokens(text: str) -> int:
        return max(1, len(text) // 4)

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


# --- Normalização / limpeza ---
_ZERO_WIDTH = r"[\u200B-\u200F\uFEFF]"
_CTRL = r"[\x00-\x08\x0B-\x0C\x0E-\x1F]"

def normalize_text(text: str) -> str:
    """Normaliza e limpa o texto mantendo parágrafos."""
    if not text:
        return ""
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(_ZERO_WIDTH, "", t)
    t = re.sub(_CTRL, " ", t)
    # Des-hifenização comum (palavra-\ncontinuação)
    t = re.sub(r"(\w)-\n(\w)", r"\1\2", t)
    # Colapsa espaços supérfluos
    t = re.sub(r"[ \t\f\v]+", " ", t)
    # Limita quebras múltiplas
    t = re.sub(r"\n{3,}", "\n\n", t)
    normalized = t.strip()
    print(f"[DEBUG] Texto normalizado: {normalized[:100]}")  # Mostra os primeiros 100 caracteres
    return normalized


# --- Classificação ---
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


# --- Segmentação (unstructured) ---
def segment_file(path: Path) -> Tuple[List, Dict]:
    """Usa o unstructured para obter elementos e metadados do documento."""
    try:
        elements = partition(filename=str(path))  # Certifique-se de que o caminho é local
    except Exception as e:
        raise RuntimeError(f"Erro ao processar o arquivo {path}: {e}")
    
    doc_meta = {
        "doc_id": path.stem,
        "filename": path.name,
        "suffix": path.suffix.lower(),
        "parent": str(path.parent),
    }
    return elements, doc_meta


def element_text(el) -> str:
    """Extrai texto do elemento, caindo para str(el) se necessário."""
    txt = getattr(el, "text", None)
    if not txt:
        try:
            d = el.to_dict()
            txt = d.get("text") or ""
        except Exception:
            txt = str(el) or ""
    print(f"[DEBUG] Texto extraído: {txt[:100]}")  # Mostra os primeiros 100 caracteres
    return txt


def element_metadata(el) -> Dict:
    """Extrai metadados estruturados do elemento."""
    md: Dict = {}
    try:
        md_obj = getattr(el, "metadata", None)
        if md_obj and hasattr(md_obj, "to_dict"):
            md = md_obj.to_dict()
        else:
            # fallback: algumas versões retornam tudo em .to_dict()
            d = el.to_dict()
            md = d.get("metadata", {}) if isinstance(d, dict) else {}
    except Exception:
        pass
    # remove campos muito pesados/voláteis
    for k in ("coordinates", "languages", "data_source"):
        if k in md and (md[k] is None or md[k] == {}):
            md.pop(k, None)
    return md


# --- Transformação em chunks ---
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
                # "source_path": os.path.join(doc_meta["parent"], doc_meta["filename"]),
                "type": cat,                     # Classificação (título, parágrafo, tabela, etc.)
                "text": part,                    # Texto normalizado/limpo
                "n_tokens": count_tokens(part),  # contagem aprox. p/ tuning
                "metadata": {
                    **emd,
                    **doc_meta,
                    "element_index": idx,
                    "chunk_index": part_idx,
                },
            }


# --- Coleta de arquivos (arquivo único OU pasta) ---
VALID_SUFFIXES = {
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".html", ".htm", ".xml",
    ".eml", ".msg",
    ".txt", ".md", ".rtf",
}

def iter_paths(root: Path):
    """Aceita uma pasta OU um arquivo único."""
    if root.is_file():
        if root.suffix.lower() in VALID_SUFFIXES:
            yield root
        else:
            print(f"[AVISO] Ignorando arquivo com extensão não suportada")
        return
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in VALID_SUFFIXES:
            yield p


# --- Execução do pipeline ---
def run_pipeline(input_path: Path, output_path: Path, max_tokens: int, overlap: int) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_docs = 0
    total_chunks = 0

    with output_path.open("w", encoding="utf-8") as out:
        for file_path in iter_paths(input_path):
            total_docs += 1
            try:
                elements, doc_meta = segment_file(file_path)
                print(f"[DEBUG] {file_path.name}: {len(elements)} elementos extraídos.")
                for el in elements:
                    print(f"[DEBUG] Elemento: {el}")
            except Exception as e:
                print(f"[ERRO] {file_path}: {e}")
                continue

            if PREVIEW and elements:
                # Mostra categoria + início do primeiro elemento
                first_txt = (element_text(elements[0]) or "")[:120].replace("\n", " ")
                first_cat = getattr(elements[0], "category", "?")
                # print(f"[INFO] {file_path.name}: {len(elements)} elementos | 1º={first_cat} :: {first_txt!r}")

            doc_chunks = 0
            for chunk in elements_to_chunks(elements, doc_meta, max_tokens, overlap):
                out.write(json.dumps(chunk, ensure_ascii=False) + "\n")
                total_chunks += 1
                doc_chunks += 1

            if doc_chunks == 0:
                print(f"[AVISO] 0 chunks gerados para (verifique conteúdo/encoding).")

    if total_docs == 0:
        print(f"[ATENÇÃO] Nenhum arquivo encontrado. "
              f"Use uma PASTA ou aponte direto para o arquivo suportado.")

    print(f"OK - Processados {total_docs} documentos, "
          f"gerados {total_chunks} chunks.")


if __name__ == "__main__":
    in_path = Path(INPUT_PATH)
    out_path = Path(OUTPUT_PATH)
    run_pipeline(in_path, out_path, MAX_TOKENS, OVERLAP)
