import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from pathlib import Path
from utils.extract_transform_script import process_directory

from utils.neo4j_ingest import ingest_incident_jsonl
from dotenv import load_dotenv

load_dotenv()
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

sample_input = PROJECT_ROOT / "coletor_data" / "data" / "external"
# sample_input = PROJECT_ROOT / "coletor_data" /"data" / "incidents"
sample_output = PROJECT_ROOT / "coletor_data" / "data" / "chunks_jsonl"

print(f"Input dir: {sample_input}")
print(f"Output dir: {sample_output}")


# TODO: Precisamos ver esse processo de pegar os arquivos sobre resposta a incidentes, fazer os devidos relacionamentos com seus IDs para depois seguir com o fluxo do unstructured, embeddings e neo4j.
# Por hora esse process_directory está apenas fazendo o unstructured (particionamento e chunking) de vários arquivos diversos em um diretório e salvando seus jsonl.
process_directory(sample_input, sample_output, recursive=True, overwrite=False)

# Aqui usamos um arquivo jsonl exemplo fictício para fazer a ingestão no neo4j.
# ingest_incident_jsonl(PROJECT_ROOT / "coletor_data" / "data" / "chunks_jsonl" / "incident_responses.jsonl")