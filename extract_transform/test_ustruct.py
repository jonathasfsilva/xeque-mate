from pathlib import Path
from extract_transform import process_directory


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

NAME_FILE = "NIST.SP.800-61r3.pdf"
FILE_TO_PROCESS = PROJECT_ROOT / "coletor_data" / "data" / "external" / "acao_recomendada" / NAME_FILE

sample_input = PROJECT_ROOT / "coletor_data" / "data" / "external"
sample_output = PROJECT_ROOT / "coletor_data" / "data" / "chunks_jsonl"

print(f"Input dir: {sample_input}")
print(f"Output dir: {sample_output}")

process_directory(sample_input, sample_output, recursive=True, overwrite=True)