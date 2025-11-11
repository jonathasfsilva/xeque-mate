# embed_openai.py
# -*- coding: utf-8 -*-
"""
Lê JSONL do pipeline Unstructured e gera embeddings (OpenAI) em lote.
Saída: JSONL com mesmo registro + campo 'embedding' (lista de floats).
"""

import os
import sys
import time
import ujson as json
from pathlib import Path
from typing import List, Dict, Iterable

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent  # sobe 1 nível (ajuste se precisar)
print(f"PROJECT_ROOT: {PROJECT_ROOT}")

# =========================
# CONFIG: edite aqui
# =========================
# INPUT_JSONL   = PROJECT_ROOT / "unstructured" / "data" / "unstruct-output" / "out.jsonl"  # JSONL vindo do unstructured_pipeline
INPUT_JSONL   = PROJECT_ROOT / "data_unstruct" / "data" / "unstruct-output" / "Relatorio_Incidente_Ransomware_ACME_IR-2025-041__20251111-113246_txt.jsonl"  # JSONL vindo do unstructured_pipeline
OUTPUT_JSONL  = PROJECT_ROOT / "embading" / "data" / "out_embedded_new.jsonl"  # saída com embeddings
EMBED_MODEL   = "text-embedding-3-small"                  # alinhe com seu índice vetorial (Neo4j)
BATCH_SIZE    = 64                                        # tamanho do lote p/ API
MAX_TOKENS_IN = 8000                                      # corte de segurança por entrada
DEDUP_BY_TEXT = True                                      # evita embutir textos idênticos
PREVIEW_LOG   = True                                      # mostra progresso a cada lote
# =========================

# torna o print seguro em consoles cp1252
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

def log(msg: str) -> None:
    try:
        print(msg)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, "encoding", "utf-8")
        safe = msg.encode(enc, errors="replace").decode(enc, errors="replace")
        sys.stdout.write(safe + "\n")

# ---- Tokenização (para truncar entradas muito grandes) ----
try:
    import tiktoken
    _ENC = tiktoken.get_encoding("cl100k_base")
    def truncate(text: str, max_tokens: int) -> str:
        toks = _ENC.encode(text)
        if len(toks) <= max_tokens:
            return text
        return _ENC.decode(toks[:max_tokens])
except Exception:
    def truncate(text: str, max_tokens: int) -> str:
        # fallback grosseiro por caracteres (~4 chars/tok)
        max_chars = max_tokens * 4
        return text if len(text) <= max_chars else text[:max_chars]

# ---- OpenAI SDK v1 ----
from openai import OpenAI
from openai import APIError, RateLimitError, APIConnectionError, APITimeoutError

client = None

def init_openai() -> None:
    global client
    load_dotenv()
    # Requer OPENAI_API_KEY no ambiente ou .env
    client = OpenAI()

def read_jsonl(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def write_jsonl(path: Path, rows: Iterable[Dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def batched(seq: List, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def embed_batch(texts: List[str], tries: int = 6, sleep_initial: float = 1.0) -> List[List[float]]:
    delay = sleep_initial
    for attempt in range(tries):
        try:
            resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
            return [d.embedding for d in resp.data]
        except (RateLimitError, APIError, APIConnectionError, APITimeoutError) as e:
            log(f"[WARN] tentativa {attempt+1}/{tries} falhou: {type(e).__name__} -> retry em {delay:.1f}s")
            time.sleep(delay)
            delay = min(delay * 2, 20)
    # última tentativa sem capturar para propagar erro
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]

def main():
    init_openai()
    in_path  = Path(INPUT_JSONL)
    out_path = Path(OUTPUT_JSONL)

    if not in_path.exists():
        log(f"[ERRO] Arquivo de entrada não encontrado: {in_path}")
        sys.exit(1)

    # Carrega registros
    records: List[Dict] = []
    seen_texts = set()
    for rec in read_jsonl(in_path):
        text = (rec.get("text") or "").strip()
        if not text:
            continue
        text = truncate(text, MAX_TOKENS_IN)
        if DEDUP_BY_TEXT:
            key = hash(text)
            if key in seen_texts:
                continue
            seen_texts.add(key)
        rec["_text_for_embed"] = text
        records.append(rec)

    if not records:
        log("[ATENÇÃO] Nenhum registro com 'text' encontrado na entrada.")
        write_jsonl(out_path, [])
        return

    log(f"[INFO] Registros para embed: {len(records)} (batch={BATCH_SIZE}) | modelo={EMBED_MODEL}")

    # Processa em lotes e escreve incrementalmente
    out_path.parent.mkdir(parents=True, exist_ok=True)
    done = 0
    with out_path.open("w", encoding="utf-8") as f:
        for batch in batched(records, BATCH_SIZE):
            texts = [r["_text_for_embed"] for r in batch]
            vectors = embed_batch(texts)

            for r, emb in zip(batch, vectors):
                r.pop("_text_for_embed", None)
                r["embedding"] = emb
                r["embedding_model"] = EMBED_MODEL
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
                done += 1

            if PREVIEW_LOG:
                log(f"[INFO] processados: {done}/{len(records)}")

    # Confere dimensão do vetor
    dim = len(records[0].get("embedding", []) or vectors[0])
    log(f"OK: {done} embeddings gerados (dim={len(vectors[0]) if 'vectors' in locals() else 'n/d'}) -> {out_path}")

if __name__ == "__main__":
    main()
