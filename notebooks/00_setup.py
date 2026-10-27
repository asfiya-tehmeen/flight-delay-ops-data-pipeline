# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the Unity Catalog objects: catalog `flight_ops`, schemas
# MAGIC `bronze` / `silver` / `gold`, and a Volume for raw file landing.
# MAGIC Idempotent: safe to re-run.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# On Free Edition you may not be allowed to create a catalog. If this fails,
# use the default workspace catalog and change CATALOG in config.py.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in (BRONZE, SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

# COMMAND ----------

import os
for d in (FLIGHTS_DIR, REFERENCE_DIR):
    os.makedirs(d, exist_ok=True)

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual step: land one month of BTS data
# MAGIC 1. https://transtats.bts.gov/ -> Aviation -> *Marketing Carrier On-Time
# MAGIC    Performance (Beginning January 2018)*.
# MAGIC 2. Select year 2024 / January, tick **all** columns, download (ZIP).
# MAGIC 3. Upload the ZIP to the Volume folder printed below
# MAGIC    (Catalog -> flight_ops -> bronze -> landing -> bts -> Upload).
# MAGIC 4. Download `airports.dat` from https://openflights.org/data.html
# MAGIC    (or the raw GitHub copy) and upload to the `reference` folder.

# COMMAND ----------

print("Upload BTS ZIP/CSV to:", FLIGHTS_DIR)
print("Upload airports.dat to:", REFERENCE_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 0 checkpoint
# MAGIC Row count for one month should be roughly 540-620K for January.

# COMMAND ----------

import zipfile, glob

for z in glob.glob(f"{FLIGHTS_DIR}/*.zip"):
    with zipfile.ZipFile(z) as zf:
        zf.extractall(FLIGHTS_DIR)

csvs = glob.glob(f"{FLIGHTS_DIR}/*.csv")
assert csvs, f"No CSVs found in {FLIGHTS_DIR}"
df = spark.read.option("header", True).csv(csvs)
print(f"{len(csvs)} file(s), {df.count():,} rows, {len(df.columns)} columns")
display(df.limit(5))
