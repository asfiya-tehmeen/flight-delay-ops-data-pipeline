# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: flights
# MAGIC BTS CSVs -> `bronze.flights_raw`, append-only.
# MAGIC
# MAGIC * Nothing is renamed, retyped or filtered. Every column is a string.
# MAGIC * `_source_file` and `_ingested_at` record provenance.
# MAGIC * Idempotent: a file already present in bronze is skipped, so re-running
# MAGIC   never duplicates rows.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import glob, os, zipfile
from pyspark.sql import functions as F

TABLE = f"{BRONZE}.flights_raw"

# Unzip anything not yet extracted (Spark cannot read ZIP directly).
for z in glob.glob(f"{FLIGHTS_DIR}/*.zip"):
    with zipfile.ZipFile(z) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv") and not os.path.exists(f"{FLIGHTS_DIR}/{name}"):
                zf.extract(name, FLIGHTS_DIR)

csv_files = sorted(os.path.basename(p) for p in glob.glob(f"{FLIGHTS_DIR}/*.csv"))
assert csv_files, f"No CSV files in {FLIGHTS_DIR}"

# COMMAND ----------

already = set()
if spark.catalog.tableExists(TABLE):
    already = {r[0] for r in spark.table(TABLE).select("_source_file").distinct().collect()}

todo = [f for f in csv_files if f not in already]
print("already loaded:", sorted(already))
print("to load:", todo)

# COMMAND ----------

for fname in todo:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)      # bronze mirrors the source as strings
        .csv(f"{FLIGHTS_DIR}/{fname}")
        .withColumn("_source_file", F.lit(fname))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    # BTS CSVs end each row with a trailing comma -> an empty unnamed column.
    df = df.drop(*[c for c in df.columns if c.startswith("_c")])
    # Delta rejects whitespace in column names and BTS headers contain some
    # (e.g. "Operating_Airline "). Trim the header only; values are untouched.
    df = df.toDF(*[c.strip() for c in df.columns])
    (df.write.format("delta").mode("append")
       .option("mergeSchema", "true").saveAsTable(TABLE))
    print("loaded", fname)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC Re-running the notebook must leave the count unchanged.

# COMMAND ----------

display(spark.table(TABLE).groupBy("_source_file").count())
