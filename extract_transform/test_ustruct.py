from pathlib import Path
from extract_transform import process_directory

from neo4j_ingest import ingest_incident_jsonl
from dotenv import load_dotenv

load_dotenv()
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

sample_input = PROJECT_ROOT / "coletor_data" / "data" / "external"
sample_output = PROJECT_ROOT / "coletor_data" / "data" / "chunks_jsonl"

print(f"Input dir: {sample_input}")
print(f"Output dir: {sample_output}")

# process_directory(sample_input, sample_output, recursive=True, overwrite=False)

ingest_incident_jsonl(PROJECT_ROOT / "coletor_data" / "data" / "external" / "incident_responses.jsonl")