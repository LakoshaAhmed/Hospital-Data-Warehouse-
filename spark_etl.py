"""
PySpark ETL: raw CSV (HDFS)  ->  staging  ->  core star schema (Parquet on HDFS).

Layers
  raw      CSV files exactly as the source systems delivered them
  staging  cleaned, typed and de-duplicated copies of each source table
  core     star schema for the warehouse: fact_operations + dimensions

The core tables are then loaded into ClickHouse (scripts/load_to_clickhouse.sh),
where the data marts are built.

Usage (inside the spark container):
    spark-submit src/spark_etl.py --base hdfs://namenode:9000/hospital
or on local files without HDFS:
    spark-submit src/spark_etl.py --base data
"""

import argparse
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

RAW_SCHEMAS = {
    "hospitals": T.StructType([
        T.StructField("hospital_id", T.IntegerType()),
        T.StructField("name", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("phone", T.StringType()),
    ]),
    "doctors": T.StructType([
        T.StructField("doctor_id", T.IntegerType()),
        T.StructField("hospital_id", T.IntegerType()),
        T.StructField("first_name", T.StringType()),
        T.StructField("last_name", T.StringType()),
        T.StructField("specialty", T.StringType()),
        T.StructField("phone", T.StringType()),
    ]),
    "patients": T.StructType([
        T.StructField("patient_id", T.IntegerType()),
        T.StructField("hospital_id", T.IntegerType()),
        T.StructField("first_name", T.StringType()),
        T.StructField("last_name", T.StringType()),
        T.StructField("date_of_birth", T.StringType()),
        T.StructField("gender", T.StringType()),
    ]),
    "operations": T.StructType([
        T.StructField("operation_id", T.LongType()),
        T.StructField("patient_id", T.IntegerType()),
        T.StructField("doctor_id", T.IntegerType()),
        T.StructField("hospital_id", T.IntegerType()),
        T.StructField("operation_name", T.StringType()),
        T.StructField("operation_date", T.StringType()),
        T.StructField("notes", T.StringType()),
    ]),
}

# Regular expressions that turn free-text notes into structured columns
RE_ALLERGY = r"(?i)allergic to ([a-z]+)"
RE_DURATION = r"(?i)(?:duration:?|op time)\s*(\d+)\s*min"
RE_BLOOD_LOSS = r"(?i)blood loss:?\s*(\d+)\s*ml"
RE_COMPLICATION = r"(?i)complication:\s*([a-z ]+?)\."

MIN_DATE, MAX_DATE = "2020-01-01", "2025-12-31"


def clean_name(col):
    """'  aortic valve REPLACEMENT ' -> 'Aortic Valve Replacement'."""
    return F.initcap(F.trim(F.regexp_replace(col, r"\s+", " ")))


def extract_or_null(col, pattern):
    value = F.regexp_extract(col, pattern, 1)
    return F.when(value == "", None).otherwise(value)


def read_raw(spark, base, name) -> DataFrame:
    return (spark.read
            .option("header", True)
            .schema(RAW_SCHEMAS[name])
            .csv(f"{base}/raw/{name}"))


# ---------- staging: clean, type and de-duplicate ----------

def stage_hospitals(df):
    return (df.dropDuplicates(["hospital_id"])
              .select("hospital_id", clean_name("name").alias("name"),
                      F.trim("city").alias("city"), "phone"))


def stage_doctors(df):
    return (df.dropDuplicates(["doctor_id"])
              .filter(F.col("hospital_id").isNotNull())
              .select("doctor_id", "hospital_id",
                      clean_name("first_name").alias("first_name"),
                      clean_name("last_name").alias("last_name"),
                      clean_name("specialty").alias("specialty")))


def stage_patients(df):
    return (df.dropDuplicates(["patient_id"])
              .withColumn("date_of_birth", F.to_date("date_of_birth"))
              .filter(F.col("date_of_birth").isNotNull())
              .select("patient_id", "hospital_id",
                      clean_name("first_name").alias("first_name"),
                      clean_name("last_name").alias("last_name"),
                      "date_of_birth", F.upper(F.trim("gender")).alias("gender")))


def stage_operations(df):
    notes = F.coalesce(F.col("notes"), F.lit(""))
    return (df.dropDuplicates(["operation_id"])
              .withColumn("operation_date", F.to_date("operation_date"))
              .filter(F.col("operation_date").between(MIN_DATE, MAX_DATE))
              .filter(F.col("patient_id").isNotNull() & F.col("doctor_id").isNotNull())
              .select(
                  "operation_id", "patient_id", "doctor_id", "hospital_id",
                  clean_name("operation_name").alias("operation_name"),
                  "operation_date",
                  extract_or_null(notes, RE_DURATION).cast("int").alias("duration_min"),
                  extract_or_null(notes, RE_BLOOD_LOSS).cast("int").alias("blood_loss_ml"),
                  F.lower(extract_or_null(notes, RE_ALLERGY)).alias("allergy"),
                  F.lower(extract_or_null(notes, RE_COMPLICATION)).alias("complication"),
                  F.col("notes"),
              ))


# ---------- core: star schema ----------

def build_dim_date(spark):
    return (spark.sql(f"SELECT explode(sequence(to_date('{MIN_DATE}'), to_date('{MAX_DATE}'))) AS date")
                 .select(
                     F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
                     "date",
                     F.year("date").alias("year"),
                     F.quarter("date").alias("quarter"),
                     F.month("date").alias("month"),
                     F.date_format("date", "MMMM").alias("month_name"),
                     F.dayofweek("date").alias("day_of_week"),  # 1 = Sunday
                     F.dayofweek("date").isin(1, 7).alias("is_weekend"),
                 ))


def build_dim_patient(patients):
    age = F.floor(F.months_between(F.lit(MAX_DATE), "date_of_birth") / 12).cast("int")
    return patients.select(
        "patient_id", "first_name", "last_name", "date_of_birth", "gender",
        F.when(age < 18, "0-17").when(age < 40, "18-39").when(age < 60, "40-59")
         .when(age < 75, "60-74").otherwise("75+").alias("age_group"),
    )


def build_fact(operations):
    return operations.select(
        "operation_id",
        F.date_format("operation_date", "yyyyMMdd").cast("int").alias("date_key"),
        "operation_date", "hospital_id", "doctor_id", "patient_id", "operation_name",
        "duration_min", "blood_loss_ml", "allergy", "complication",
        F.col("complication").isNotNull().alias("has_complication"),
    )


def write(df, base, layer, name):
    df.write.mode("overwrite").parquet(f"{base}/{layer}/{name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="data", help="root folder, e.g. hdfs://namenode:9000/hospital")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("hospital-dwh-etl")
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    t0 = time.time()
    base = args.base

    raw = {name: read_raw(spark, base, name) for name in RAW_SCHEMAS}
    raw_ops_count = raw["operations"].count()

    staged = {
        "hospitals": stage_hospitals(raw["hospitals"]),
        "doctors": stage_doctors(raw["doctors"]),
        "patients": stage_patients(raw["patients"]),
        "operations": stage_operations(raw["operations"]),
    }
    for name, df in staged.items():
        write(df, base, "staging", name)

    # read staging back so the core layer starts from the saved, cleaned data
    st = {name: spark.read.parquet(f"{base}/staging/{name}") for name in staged}

    core = {
        "dim_hospital": st["hospitals"],
        "dim_doctor": st["doctors"],
        "dim_patient": build_dim_patient(st["patients"]),
        "dim_date": build_dim_date(spark),
        "fact_operations": build_fact(st["operations"]),
    }
    for name, df in core.items():
        write(df, base, "core", name)

    fact = spark.read.parquet(f"{base}/core/fact_operations")
    fact_count = fact.count()
    print("=" * 60)
    print(f"raw operation rows      : {raw_ops_count:,}")
    print(f"fact_operations rows    : {fact_count:,}")
    print(f"removed (dupes / bad)   : {raw_ops_count - fact_count:,}")
    fact.select(
        F.round(F.avg(F.col("duration_min").isNotNull().cast("int")) * 100, 1).alias("pct_with_duration"),
        F.round(F.avg(F.col("allergy").isNotNull().cast("int")) * 100, 1).alias("pct_with_allergy"),
        F.round(F.avg(F.col("has_complication").cast("int")) * 100, 1).alias("pct_with_complication"),
    ).show()
    print(f"ETL finished in {time.time() - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
