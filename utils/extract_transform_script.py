from pathlib import Path
import json
from typing import Optional, Sequence
from unstructured.partition.auto import partition
from unstructured.chunking import basic
#from dotenv import load_dotenv

#load_dotenv()

# --- Particionamento ---

def process_directory(
    input_dir: str | Path,
    output_dir: str | Path,
    suffixes: Optional[Sequence[str]] = None,
    recursive: bool = True,
    overwrite: bool = False,
) -> None:
    """
    Percorre input_dir, gera chunks e grava um .jsonl por arquivo.
    Remove chaves 'element_id' (recursivamente) antes de gravar.
    """
    def remove_keys_recursive(obj: object, keys: set[str], metadata_keys: set[str] = None) -> object:
        if isinstance(obj, dict):
            # Remove chaves gerais
            new_obj = {
                k: remove_keys_recursive(v, keys, metadata_keys)
                for k, v in obj.items()
                if k not in keys
            }
            # Se for metadata, remove campos específicos
            if "metadata" in new_obj and isinstance(new_obj["metadata"], dict) and metadata_keys:
                new_obj["metadata"] = {
                    mk: mv
                    for mk, mv in new_obj["metadata"].items()
                    if mk not in metadata_keys
                }
            return new_obj
        if isinstance(obj, list):
            return [remove_keys_recursive(item, keys, metadata_keys) for item in obj]
        return obj

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"iniciando fluxo - input: {input_path}, output: {output_path}")
    default_suffixes = {".pdf", ".docx", ".txt", ".md", ".html", ".htm", ".pptx", ".jsonl", ".log", ".json"}
    allowed = {s.lower() for s in (suffixes or default_suffixes)}

    if recursive:
        files = input_path.rglob("*")
    else:
        files = input_path.glob("*")
        
    print(f"files to process: {files}")

    for file_path in files:
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in allowed:
            continue

        print(f"Processing: {file_path}")
        try:
            partitions = partition(filename=str(file_path))
        except Exception as e:
            print(f"  ERROR partitioning {file_path}: {e}")
            continue

        try:
            chunks = list(basic.chunk_elements(elements=partitions))
        except Exception as e:
            print(f"  ERROR chunking {file_path}: {e}")
            continue

        out_filename = f"{file_path.stem}.jsonl"
        out_file = output_path / out_filename
        if out_file.exists() and not overwrite:
            print(f"  Skipping existing output (use overwrite=True to replace): {out_file}")
            continue

        try:
            with out_file.open("w", encoding="utf-8") as fh:
                for chunk in chunks:
                    try:
                        obj = chunk.to_dict()
                    except Exception:
                        try:
                            obj = json.loads(chunk.to_json())
                        except Exception:
                            obj = {"text": getattr(chunk, "text", str(chunk)), "metadata": getattr(chunk, "metadata", {})}

                    fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            print(f"  Wrote: {out_file}")
        except Exception as e:
            print(f"  ERROR writing {out_file}: {e}")
