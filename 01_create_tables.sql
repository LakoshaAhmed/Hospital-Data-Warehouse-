-- =====================================================================
-- ClickHouse: core layer of the hospital data warehouse (star schema)
-- Column order matches the Parquet files written by src/spark_etl.py
-- =====================================================================

CREATE DATABASE IF NOT EXISTS dwh;

-- ---------- dimensions ----------

CREATE TABLE IF NOT EXISTS dwh.dim_hospital
(
    hospital_id UInt16,
    name        String,
    city        LowCardinality(String),
    phone       String
)
ENGINE = MergeTree
ORDER BY hospital_id;

CREATE TABLE IF NOT EXISTS dwh.dim_doctor
(
    doctor_id   UInt32,
    hospital_id UInt16,
    first_name  String,
    last_name   String,
    specialty   LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY doctor_id;

CREATE TABLE IF NOT EXISTS dwh.dim_patient
(
    patient_id    UInt32,
    first_name    String,
    last_name     String,
    date_of_birth Date32,
    gender        LowCardinality(String),
    age_group     LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY patient_id;

CREATE TABLE IF NOT EXISTS dwh.dim_date
(
    date_key    UInt32,
    date        Date,
    year        UInt16,
    quarter     UInt8,
    month       UInt8,
    month_name  LowCardinality(String),
    day_of_week UInt8,      -- 1 = Sunday ... 7 = Saturday
    is_weekend  Bool
)
ENGINE = MergeTree
ORDER BY date_key;

-- ---------- fact table ----------
-- PARTITION BY month: queries for a period only open the needed partitions.
-- ORDER BY (hospital_id, operation_date): the sparse primary index lets
-- ClickHouse skip blocks of rows when filtering by hospital and date.
-- LowCardinality(String) stores repeated text (operation names) as a dictionary.

CREATE TABLE IF NOT EXISTS dwh.fact_operations
(
    operation_id     UInt64,
    date_key         UInt32,
    operation_date   Date,
    hospital_id      UInt16,
    doctor_id        UInt32,
    patient_id       UInt32,
    operation_name   LowCardinality(String),
    duration_min     Nullable(UInt16),
    blood_loss_ml    Nullable(UInt16),
    allergy          LowCardinality(Nullable(String)),
    complication     LowCardinality(Nullable(String)),
    has_complication Bool
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(operation_date)
ORDER BY (hospital_id, operation_date, doctor_id);

-- Same data with no partitions, no sort key and plain String columns.
-- Used only by scripts/benchmark.py to measure the effect of the design above.
CREATE TABLE IF NOT EXISTS dwh.fact_operations_naive
(
    operation_id     UInt64,
    date_key         UInt32,
    operation_date   Date,
    hospital_id      UInt16,
    doctor_id        UInt32,
    patient_id       UInt32,
    operation_name   String,
    duration_min     Nullable(UInt16),
    blood_loss_ml    Nullable(UInt16),
    allergy          Nullable(String),
    complication     Nullable(String),
    has_complication Bool
)
ENGINE = MergeTree
ORDER BY tuple();
