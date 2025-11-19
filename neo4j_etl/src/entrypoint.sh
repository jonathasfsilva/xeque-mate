#!/bin/bash

# Run any setup steps or pre-processing tasks here
echo "Running ETL to move data from jsonl files to Neo4j..."

# Run the ETL script
python bulk_jsonl_write.py
