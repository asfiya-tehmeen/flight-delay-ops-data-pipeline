# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: airports + weather
# MAGIC 1. Load OpenFlights `airports.dat` -> `bronze.airports_raw`.
# MAGIC 2. For each origin airport seen in `bronze.flights_raw`, fetch hourly
# MAGIC    weather from Open-Meteo for the whole date range in ONE call
# MAGIC    (batched `WEATHER_BATCH_SIZE` airports per request) and store the raw
# MAGIC    JSON in `bronze.weather_raw`.
# MAGIC 3. Airports already held for the same date range are never re-requested.
# MAGIC
# MAGIC Weather data (c) Open-Meteo, CC BY 4.0.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json, time, requests
from pyspark.sql import functions as F, types as T

AIRPORTS = f"{BRONZE}.airports_raw"
WEATHER = f"{BRONZE}.weather_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Airports (reference data)

# COMMAND ----------

if not spark.catalog.tableExists(AIRPORTS):
    cols = ["airport_id", "name", "city", "country", "iata", "icao", "lat", "lon",
            "altitude_ft", "tz_offset", "dst", "tz_iana", "type", "source"]
    schema = T.StructType([T.StructField(c, T.StringType()) for c in cols])
    (spark.read.schema(schema).option("nullValue", "\\N")
        .csv(f"{REFERENCE_DIR}/airports.dat")
        .withColumn("_ingested_at", F.current_timestamp())
        .write.format("delta").saveAsTable(AIRPORTS))

print(spark.table(AIRPORTS).count(), "airports")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which airports still need weather?

# COMMAND ----------

origins = {r[0] for r in spark.table(f"{BRONZE}.flights_raw").select("Origin").distinct().collect()}

held = set()
if spark.catalog.tableExists(WEATHER):
    held = {r[0] for r in spark.table(WEATHER)
            .filter((F.col("start_date") == START_DATE) & (F.col("end_date") == END_DATE))
            .select("iata").collect()}

coords = {r["iata"]: (float(r["lat"]), float(r["lon"]))
          for r in spark.table(AIRPORTS).filter(F.col("iata").isin(list(origins))).collect()}

for iata, (lat, lon, _tz) in AIRPORT_OVERRIDES.items():
    if iata in origins and iata not in coords:
        coords[iata] = (lat, lon)

missing_coords = sorted(origins - set(coords))
if missing_coords:
    print("WARNING no coordinates for:", missing_coords)  # investigate, do not drop silently

todo = sorted(set(coords) - held)
print(f"{len(origins)} origins, {len(held)} already held, {len(todo)} to fetch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch (batched, cached in bronze)

# COMMAND ----------

def fetch(batch):
    params = {
        "latitude": ",".join(str(coords[a][0]) for a in batch),
        "longitude": ",".join(str(coords[a][1]) for a in batch),
        "start_date": START_DATE, "end_date": END_DATE,
        "hourly": ",".join(WEATHER_VARS),
        "timezone": "UTC",                 # timezone work happens on the flight side
    }
    r = requests.get(WEATHER_URL, params=params, timeout=120)
    r.raise_for_status()
    body = r.json()
    return body if isinstance(body, list) else [body]   # multi-location returns a list

out_schema = "iata string, lat double, lon double, start_date string, end_date string, raw_json string, _fetched_at timestamp"

for i in range(0, len(todo), WEATHER_BATCH_SIZE):
    batch = todo[i:i + WEATHER_BATCH_SIZE]
    results = fetch(batch)
    assert len(results) == len(batch), "Open-Meteo returned an unexpected number of locations"
    rows = [(a, coords[a][0], coords[a][1], START_DATE, END_DATE, json.dumps(res))
            for a, res in zip(batch, results)]
    (spark.createDataFrame(rows, "iata string, lat double, lon double, start_date string, end_date string, raw_json string")
        .withColumn("_fetched_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(WEATHER))
    print("fetched", batch)
    time.sleep(WEATHER_SLEEP_SEC)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC Re-running must fetch nothing. One row per airport, no duplicates.

# COMMAND ----------

w = spark.table(WEATHER)
print("rows:", w.count(), "distinct airports:", w.select("iata").distinct().count())
