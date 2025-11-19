import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from pathlib import Path
from utils.extract_transform_script import process_directory
from utils.coletor_html_mitre import run_mitre_collector
from utils.preprocessing import preprocess_jsonl

from utils.neo4j_loader import load_jsonl_to_neo4j
from dotenv import load_dotenv

load_dotenv()
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

print(f"Projeto raiz: {PROJECT_ROOT}")

# Saida dos dados coletados
OUTPUT_DIR_COLETOR = PROJECT_ROOT / "data" / "mitre_attck_html"
OUTPUT_DIR_COLETOR.mkdir(parents=True, exist_ok=True)

print("================== Início do fluxo completo =================")
print("Iniciando coleta de dados do MITRE ATT&CK...")
# Primeiro passo fazer o coletor dos dados, por enquanto vamos fazer no MITRE ATT&CK usando o script dedicado.
run_mitre_collector(output_dir=OUTPUT_DIR_COLETOR, limit=10)


# Saída dos dados processados pelo unstructured
OUTPUT_DIR_UNSTRUCTURED = PROJECT_ROOT / "data" / "unstructured_chunks_jsonl"
OUTPUT_DIR_UNSTRUCTURED.mkdir(parents=True, exist_ok=True)

print("================== Processamento com unstructured =================")
#  Segundo passo, processar esses dados coletados e chamar o unstructured para gerar os chunks.
# process_directory(input_dir=OUTPUT_DIR_COLETOR, output_dir=OUTPUT_DIR_UNSTRUCTURED, recursive=True, overwrite=False)

# Saída dos dados pré-processados
OUTPUT_DIR_PREPROCESSED = PROJECT_ROOT / "data" / "preprocessed_jsonl"
OUTPUT_DIR_PREPROCESSED.mkdir(parents=True, exist_ok=True)

print("================== Pré-processamento dos JSONL =================")
# Terceiro passo é preprocessar os JSONL gerados para limpeza e padronização.
preprocess_jsonl(raw_dir=OUTPUT_DIR_UNSTRUCTURED, processed_dir=OUTPUT_DIR_PREPROCESSED)

print("================== Ingestão no Neo4j =================")
# Ultimo passo, ingerir os dados pré-processados no Neo4j.
load_jsonl_to_neo4j(jsonl_folder=OUTPUT_DIR_PREPROCESSED)