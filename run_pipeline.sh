#!/usr/bin/env bash
# Run the whole pipeline end to end. Start the stack first: docker compose up -d
# Usage: bash scripts/run_pipeline.sh [number_of_operations]
set -euo pipefail
OPS="${1:-5000000}"

echo "== 1/4 Generating $OPS synthetic operations =="
docker exec spark python src/generate_data.py --operations "$OPS" --out data/raw

echo "== 2/4 Uploading raw CSV files to HDFS =="
bash scripts/upload_to_hdfs.sh

echo "== 3/4 Running the PySpark ETL (raw -> staging -> core) =="
docker exec spark spark-submit --driver-memory 4g src/spark_etl.py \
    --base hdfs://namenode:9000/hospital

echo "== 4/4 Loading the core layer into ClickHouse =="
bash scripts/load_to_clickhouse.sh

echo "Done. Next: python scripts/benchmark.py"
