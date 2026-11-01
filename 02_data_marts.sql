-- =====================================================================
-- ClickHouse: data mart layer
-- Materialized views fill the marts automatically on every INSERT into
-- dwh.fact_operations, so create them BEFORE loading the fact table.
-- =====================================================================

-- ---------- mart 1: daily load per hospital and operation ----------
-- SummingMergeTree adds up the numeric columns of rows with the same key
-- in the background. Always query it with sum() + GROUP BY, because
-- merging is eventual.

CREATE TABLE IF NOT EXISTS dwh.mart_daily_hospital
(
    operation_date      Date,
    hospital_id         UInt16,
    operation_name      LowCardinality(String),
    operations          UInt64,
    complications       UInt64,
    duration_sum        UInt64,
    duration_count      UInt64,
    blood_loss_sum      UInt64,
    blood_loss_count    UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYear(operation_date)
ORDER BY (hospital_id, operation_date, operation_name);

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_daily_hospital
TO dwh.mart_daily_hospital
AS
SELECT
    operation_date,
    hospital_id,
    operation_name,
    count()                       AS operations,
    countIf(has_complication)     AS complications,
    sum(ifNull(duration_min, 0))  AS duration_sum,
    count(duration_min)           AS duration_count,
    sum(ifNull(blood_loss_ml, 0)) AS blood_loss_sum,
    count(blood_loss_ml)          AS blood_loss_count
FROM dwh.fact_operations
GROUP BY operation_date, hospital_id, operation_name;

-- ---------- mart 2: monthly workload per doctor ----------
-- AggregatingMergeTree keeps partial aggregate states, which lets us store
-- "unique patients" correctly and merge it across any period later.

CREATE TABLE IF NOT EXISTS dwh.mart_doctor_monthly
(
    month           Date,
    doctor_id       UInt32,
    operations      SimpleAggregateFunction(sum, UInt64),
    complications   SimpleAggregateFunction(sum, UInt64),
    unique_patients AggregateFunction(uniq, UInt32)
)
ENGINE = AggregatingMergeTree
ORDER BY (doctor_id, month);

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_doctor_monthly
TO dwh.mart_doctor_monthly
AS
SELECT
    toStartOfMonth(operation_date) AS month,
    doctor_id,
    count()                        AS operations,
    countIf(has_complication)      AS complications,
    uniqState(patient_id)          AS unique_patients
FROM dwh.fact_operations
GROUP BY month, doctor_id;
