# save as fix_csv_simple.py
import sys
sys.path.append('.')
from pathlib import Path
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent 
print(f"PROJECT_ROOT: {PROJECT_ROOT}")

INPUT_CSV = PROJECT_ROOT / "data" / "Ransomware.csv"  # caminho do seu CSV original
OUTPUT_CSV = PROJECT_ROOT / "data" / "Ransomware_clean.csv"  # caminho de saída (vírgula, UTF-8)
# ===================

# tenta ler em UTF-8; se falhar, tenta latin1 (Windows)
try:
    df = pd.read_csv(INPUT_CSV, sep="|", engine="python", encoding="utf-8")
except Exception:
    df = pd.read_csv(INPUT_CSV, sep="|", engine="python", encoding="latin1")

# limpeza leve
df.columns = [str(c).strip() for c in df.columns]              # tira espaços nos nomes das colunas
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # tira espaços em células de texto

# cria a pasta de saída se não existir e salva com vírgula
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8", sep=";")

print(f"Arquivo limpo salvo em: {OUTPUT_CSV.resolve()}")
print(f"Linhas: {len(df)} | Colunas: {df.shape[1]}")
