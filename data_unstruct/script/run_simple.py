# -*- coding: utf-8 -*-
# run_simple.py — versão com type hints p/ Pylance

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Sequence, Optional, Dict, Any

from unstructured.partition.auto import partition
from unstructured.documents.elements import Element  # <- classes de elementos

# ========= EDITE AQUI =========
# FILE: str = "./example-docs/pdf/fake-memo.pdf" # PDF
# FILE: str = "./example-docs/img/table-multi-row-column-cells.png"  # IMG
# STRATEGY: Optional[str] = None    # ex.: "hi_res" p/ PDFs 
# OCR_LANG: Optional[str] = None    # ex.: "por" ou "por+eng"
# OCR_LANG: Optional[str] = "por"  # texto 

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent  # sobe 1 nível (ajuste se precisar)

print(f"PROJECT_ROOT: {PROJECT_ROOT}")

FILE: str = PROJECT_ROOT / "data" / "Ransomware_clean.csv"  # csv
OUTPUT_DIR: str = PROJECT_ROOT / "data" / "unstruct-output"
# OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
# FILE: str = "./data_unstruct/data/Ransomware.csv"  # csv
# OUTPUT_DIR: str = "./data_unstruct/data/unstruct-output"

STRATEGY: Optional[str] = "hi_res"    # ex.: "hi_res" p/ PDFs
LANGUAGES: Optional[list[str]] = ["por"]  # PDF
# ==============================

def to_jsonl(elements: Sequence[Element], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for el in elements:
            f.write(json.dumps(el.to_dict(), ensure_ascii=False) + "\n")

def main() -> None:
    kwargs: Dict[str, Any] = {}
    if STRATEGY:
        kwargs["strategy"] = STRATEGY
    if LANGUAGES:
        kwargs["languages"] = LANGUAGES

    in_path = Path(FILE)
    elements: list[Element] = list(partition(filename=str(in_path), **kwargs))

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    ext = in_path.suffix.lower().lstrip(".") or "unknown"
    out_file = Path(OUTPUT_DIR) / f"{in_path.stem}__{ts}_{ext}.jsonl"
    to_jsonl(elements, out_file)

    print(f"\nOK: {in_path} -> {out_file}")
    print(f"Total de elementos: {len(elements)}\n")
    for e in elements[:8]:
        print(f"{e.category:14} => {(e.text or '')[:80]}")

if __name__ == "__main__":
    main()
