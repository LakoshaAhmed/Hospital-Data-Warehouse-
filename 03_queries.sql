-- =====================================================================
-- ClickHouse: analytical queries on the hospital data warehouse
-- =====================================================================

-- 1. Monthly number of operations and complication rate (star-schema join)
SELECT
    d.year,
    d.month,
    count()                                   AS operations,
    round(100 * countIf(f.has_complication) / count(), 2)   AS complication_rate_pct
FROM dwh.fact_operations AS f
INNER JOIN dwh.dim_date AS d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2. Complication rate by doctor specialty
SELECT
    doc.specialty,
    count()                                   AS operations,
    countIf(f.has_complication)               AS complications,
    round(100 * complications / operations, 2) AS complication_rate_pct
FROM dwh.fact_operations AS f
INNER JOIN dwh.dim_doctor AS doc ON f.doctor_id = doc.doctor_id
GROUP BY doc.specialty
ORDER BY complication_rate_pct DESC;

-- 3. Average and 90th percentile of duration and blood loss per operation type
SELECT
    operation_name,
    count()                          AS operations,
    round(avg(duration_min))         AS avg_duration_min,
    quantile(0.9)(duration_min)      AS p90_duration_min,
    round(avg(blood_loss_ml))        AS avg_blood_loss_ml
FROM dwh.fact_operations
GROUP BY operation_name
ORDER BY avg_duration_min DESC;

-- 4. Most common allergies (extracted from free-text notes by regex in Spark)
SELECT
    allergy,
    count()                                                   AS operations,
    round(100 * count() / (SELECT count() FROM dwh.fact_operations), 2) AS share_pct
FROM dwh.fact_operations
WHERE allergy IS NOT NULL
GROUP BY allergy
ORDER BY operations DESC;

-- 5. Top 10 doctors by operations in 2025, with their rank inside their hospital
SELECT *
FROM
(
    SELECT
        doc.hospital_id,
        f.doctor_id,
        concat(doc.first_name, ' ', doc.last_name) AS doctor_name,
        count()                                    AS operations,
        rank() OVER (PARTITION BY doc.hospital_id ORDER BY count() DESC) AS rank_in_hospital
    FROM dwh.fact_operations AS f
    INNER JOIN dwh.dim_doctor AS doc ON f.doctor_id = doc.doctor_id
    WHERE f.operation_date BETWEEN '2025-01-01' AND '2025-12-31'
    GROUP BY doc.hospital_id, f.doctor_id, doctor_name
)
ORDER BY operations DESC
LIMIT 10;

-- 6. Running total of operations per hospital in 2025 (window function on the mart)
SELECT
    hospital_id,
    toStartOfMonth(operation_date)          AS month,
    sum(operations)                         AS monthly_operations,
    sum(sum(operations)) OVER (PARTITION BY hospital_id ORDER BY month) AS running_total
FROM dwh.mart_daily_hospital
WHERE operation_date >= '2025-01-01'
GROUP BY hospital_id, month
ORDER BY hospital_id, month;

-- 7. Complication rate by patient age group and gender
SELECT
    p.age_group,
    p.gender,
    count()                                 AS operations,
    round(100 * countIf(f.has_complication) / count(), 2) AS complication_rate_pct
FROM dwh.fact_operations AS f
INNER JOIN dwh.dim_patient AS p ON f.patient_id = p.patient_id
GROUP BY p.age_group, p.gender
ORDER BY p.age_group, p.gender;

-- 8. Doctor workload from the AggregatingMergeTree mart (unique patients per year)
SELECT
    doctor_id,
    toYear(month)               AS year,
    sum(operations)             AS total_operations,
    uniqMerge(unique_patients)  AS patients
FROM dwh.mart_doctor_monthly
GROUP BY doctor_id, year
ORDER BY total_operations DESC
LIMIT 10;

-- 9. Weekday vs weekend load
SELECT
    d.is_weekend,
    count()                              AS operations,
    round(count() / uniqExact(d.date))   AS avg_operations_per_day
FROM dwh.fact_operations AS f
INNER JOIN dwh.dim_date AS d ON f.date_key = d.date_key
GROUP BY d.is_weekend;

-- =====================================================================
-- Query optimization: check that the primary index and partitions are used
-- =====================================================================

-- Shows how many parts and granules ClickHouse skips thanks to
-- PARTITION BY toYYYYMM(operation_date) and ORDER BY (hospital_id, operation_date, ...)
EXPLAIN indexes = 1
SELECT count()
FROM dwh.fact_operations
WHERE hospital_id = 5
  AND operation_date BETWEEN '2025-03-01' AND '2025-03-31';

-- The same filter on the naive table has to read every row
EXPLAIN indexes = 1
SELECT count()
FROM dwh.fact_operations_naive
WHERE hospital_id = 5
  AND operation_date BETWEEN '2025-03-01' AND '2025-03-31';
