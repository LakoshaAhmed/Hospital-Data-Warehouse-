#!/usr/bin/env bash
# Upload the generated raw CSV files from ./data/raw into HDFS (/hospital/raw).
set -euo pipefail

docker exec namenode hdfs dfs -mkdir -p /hospital/raw
docker exec namenode hdfs dfs -put -f /data/raw/hospitals /data/raw/doctors \
    /data/raw/patients /data/raw/operations /hospital/raw/

echo "Files in HDFS:"
docker exec namenode hdfs dfs -du -h /hospital/raw
