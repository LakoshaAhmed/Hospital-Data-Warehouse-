#!/usr/bin/env bash
# Create the ClickHouse tables and marts, then load the core Parquet files
# written by Spark straight from HDFS with ClickHouse's hdfs() table function.
set -euo pipefail

CH="docker exec -i clickhouse clickhouse-client --password clickhouse --multiquery"
CORE="hdfs://namenode:9000/hospital/core"

echo "Creating tables and data marts..."
$CH < sql/01_create_tables.sql
$CH < sql/02_data_marts.sql

echo "Clearing old data (safe to re-run)..."
for t in dim_hospital dim_doctor dim_patient dim_date fact_operations fact_operations_naive \
         mart_daily_hospital mart_doctor_monthly; do
  $CH --query "TRUNCATE TABLE IF EXISTS dwh.$t"
done

echo "Loading dimensions..."
for t in dim_hospital dim_doctor dim_patient dim_date; do
  $CH --query "INSERT INTO dwh.$t SELECT * FROM hdfs('$CORE/$t/*.parquet', 'Parquet')"
done

echo "Loading fact table (the materialized views fill the marts at the same time)..."
$CH --query "INSERT INTO dwh.fact_operations SELECT * FROM hdfs('$CORE/fact_operations/*.parquet', 'Parquet')"

echo "Copying the fact table into the naive table for the benchmark..."
$CH --query "INSERT INTO dwh.fact_operations_naive SELECT * FROM dwh.fact_operations"

echo
echo "Row counts:"
$CH --query "
SELECT table, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE database = 'dwh' AND active
GROUP BY table
ORDER BY table
FORMAT PrettyCompact"
