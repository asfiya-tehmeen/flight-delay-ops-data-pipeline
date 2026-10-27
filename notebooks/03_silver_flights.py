# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver: flights
# MAGIC `bronze.flights_raw` -> `silver.dim_airport`, `silver.flights_completed`,
# MAGIC `silver.flights_disrupted`.
# MAGIC
# MAGIC **Timestamp strategy** (the hard part):
# MAGIC * BTS times are local `HHMM` integers with no offset.
# MAGIC * Scheduled departure = `FlightDate` + `CRSDepTime`, interpreted in the
# MAGIC   ORIGIN airport's IANA timezone -> UTC.
# MAGIC * Scheduled arrival = scheduled departure (UTC) + `CRSElapsedTime`. This
# MAGIC   sidesteps overnight flights and cross-timezone arithmetic entirely: the
# MAGIC   arrival date is never guessed.
# MAGIC * Cross-check: convert derived arrival back to DEST local time and compare
# MAGIC   with `CRSArrTime`. The mismatch rate is a quality gate.
# MAGIC * `2400` is treated as midnight of the next day (minutes-since-midnight
# MAGIC   arithmetic handles this without special-casing).
# MAGIC
# MAGIC Silver is rebuilt from bronze on every run (overwrite), so it is idempotent.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F, Window

# Wall-clock arithmetic below builds timestamps from local clock values, so the
# session zone must be UTC or to_utc_timestamp() would shift them twice.
spark.conf.set("spark.sql.session.timeZone", "UTC")
assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

bronze = spark.table(f"{BRONZE}.flights_raw")
print("bronze rows:", bronze.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Airport dimension (lat/lon + IANA timezone)

# COMMAND ----------

used = (bronze.select(F.col("Origin").alias("iata"))
        .union(bronze.select(F.col("Dest").alias("iata"))).distinct())

w = Window.partitionBy("iata").orderBy(F.col("airport_id").cast("int"))
openflights = (
    spark.table(f"{BRONZE}.airports_raw")
    .filter(F.col("iata").isNotNull())
    .join(used, "iata")
    .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    .select("iata", "name", "city", "country",
            F.col("lat").cast("double").alias("lat"),
            F.col("lon").cast("double").alias("lon"),
            F.col("tz_iana"), F.lit("openflights").alias("source"))
)

overrides = spark.createDataFrame(
    [(k, None, None, None, v[0], v[1], v[2], "override") for k, v in AIRPORT_OVERRIDES.items()],
    "iata string, name string, city string, country string, lat double, lon double, tz_iana string, source string",
).select("iata", "name", "city", "country", "lat", "lon", "tz_iana", "source")

dim_airport = (openflights.join(overrides.select("iata"), "iata", "left_anti")
               .unionByName(overrides.join(used, "iata")))

dim_airport.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER}.dim_airport")
dim_airport = spark.table(f"{SILVER}.dim_airport")

no_tz = used.join(dim_airport.filter("tz_iana IS NOT NULL"), "iata", "left_anti")
print("airports in scope:", used.count(), "| with timezone:", dim_airport.filter("tz_iana IS NOT NULL").count())
print("airports WITHOUT timezone (add to AIRPORT_OVERRIDES):", [r[0] for r in no_tz.collect()])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explicit typing (no schema inference)
# MAGIC Casts use `try_cast`, so a malformed value becomes NULL instead of
# MAGIC crashing, and the *cast-loss* gate below reports any value that was
# MAGIC present in bronze but lost in the cast.
# MAGIC
# MAGIC Deliberately not carried into silver: the Div2-Div5 diversion detail,
# MAGIC the DOT/SeqID/WAC/FIPS code columns and the delay-group buckets.

# COMMAND ----------

# (bronze column, silver column, type)
COLUMNS = [
    ("FlightDate", "flight_date", "date"),
    ("Year", "year", "int"), ("Quarter", "quarter", "int"), ("Month", "month", "int"),
    ("DayofMonth", "day_of_month", "int"), ("DayOfWeek", "day_of_week", "int"),  # 1=Mon..7=Sun
    ("Marketing_Airline_Network", "marketing_carrier", "string"),
    ("Operating_Airline", "operating_carrier", "string"),
    ("Flight_Number_Marketing_Airline", "flight_number", "int"),
    ("Tail_Number", "tail_number", "string"),
    ("Origin", "origin", "string"), ("OriginCityName", "origin_city", "string"),
    ("OriginState", "origin_state", "string"),
    ("Dest", "dest", "string"), ("DestCityName", "dest_city", "string"),
    ("DestState", "dest_state", "string"),
    # schedule (knowable before departure)
    ("CRSDepTime", "crs_dep_hhmm", "int"), ("CRSArrTime", "crs_arr_hhmm", "int"),
    ("CRSElapsedTime", "crs_elapsed_min", "int"), ("Distance", "distance_miles", "double"),
    # outcomes (reporting only - never model features)
    ("DepTime", "dep_hhmm", "int"), ("DepDelay", "dep_delay_min", "int"),
    ("DepDel15", "dep_del15", "int"),
    ("TaxiOut", "taxi_out_min", "int"), ("WheelsOff", "wheels_off_hhmm", "int"),
    ("WheelsOn", "wheels_on_hhmm", "int"), ("TaxiIn", "taxi_in_min", "int"),
    ("ArrTime", "arr_hhmm", "int"), ("ArrDelay", "arr_delay_min", "int"),
    ("ArrDel15", "arr_del15", "int"),
    ("Cancelled", "cancelled", "int"), ("CancellationCode", "cancellation_code", "string"),
    ("Diverted", "diverted", "int"), ("DivAirportLandings", "div_airport_landings", "int"),
    ("ActualElapsedTime", "actual_elapsed_min", "int"), ("AirTime", "air_time_min", "int"),
    ("CarrierDelay", "carrier_delay_min", "int"), ("WeatherDelay", "weather_delay_min", "int"),
    ("NASDelay", "nas_delay_min", "int"), ("SecurityDelay", "security_delay_min", "int"),
    ("LateAircraftDelay", "late_aircraft_delay_min", "int"),
    ("Duplicate", "bts_duplicate_flag", "string"),
]

def typed(src, dst, typ):
    c = F.col(src)
    if typ == "string":
        return F.trim(c).alias(dst)
    if typ == "date":   # tolerate both BTS date renderings
        return F.expr(
            f"CAST(coalesce(try_to_timestamp(`{src}`, 'yyyy-MM-dd'), "
            f"try_to_timestamp(`{src}`, 'M/d/yyyy h:mm:ss a')) AS DATE)").alias(dst)
    if typ == "int":    # BTS writes integers as "5.00"
        return F.expr(f"CAST(try_cast(`{src}` AS DOUBLE) AS INT)").alias(dst)
    return F.expr(f"try_cast(`{src}` AS {typ})").alias(dst)

# a blank string is a missing value, not a value
base = bronze.select(*[F.when(F.trim(F.col(s)) == "", None).otherwise(F.col(s)).alias(s)
                       for s, _, _ in COLUMNS], "_source_file")
flights = base.select(*[typed(*c) for c in COLUMNS], "_source_file")

# cast-loss measure: present in bronze, NULL after the cast
cast_loss = base.select(*[
    F.sum(F.when(F.col(s).isNotNull() & typed(s, d, t).isNull(), 1).otherwise(0)).alias(d)
    for s, d, t in COLUMNS if t != "string"]).first().asDict()
print({k: v for k, v in cast_loss.items() if v})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Duplicates and grain
# MAGIC Grain: one row per (flight_date, marketing_carrier, flight_number, origin,
# MAGIC dest, crs_dep_hhmm). Identical rows are dropped; anything that still
# MAGIC collides on the key is NOT silently dropped - the grain gate fails.

# COMMAND ----------

print("BTS duplicate flag values:")
display(flights.groupBy("bts_duplicate_flag").count())

n0 = flights.count()
if EXCLUDE_DUPLICATE_FLAG:
    flights = flights.filter(F.coalesce(F.col("bts_duplicate_flag"), F.lit("N")) != "Y")
n1 = flights.count()
flights = flights.drop("_source_file").dropDuplicates()
n2 = flights.count()
print(f"rows {n0:,} -> {n1:,} after BTS duplicate flag -> {n2:,} after exact-duplicate removal")

KEY = ["flight_date", "marketing_carrier", "flight_number", "origin", "dest", "crs_dep_hhmm"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Timestamps

# COMMAND ----------

def minutes_of_day(c):          # HHMM int -> minutes since midnight (2400 -> 1440)
    return F.floor(c / 100) * 60 + (c % 100)

d_tz = dim_airport.select(F.col("iata").alias("origin"), F.col("tz_iana").alias("origin_tz"))
a_tz = dim_airport.select(F.col("iata").alias("dest"), F.col("tz_iana").alias("dest_tz"))

def epoch(c):
    return F.unix_timestamp(c)

silver = (
    flights
    .join(F.broadcast(d_tz), "origin", "left")
    .join(F.broadcast(a_tz), "dest", "left")
    # local wall clock, deliberately built as if UTC (session zone is UTC)
    .withColumn("_dep_wall", (epoch(F.col("flight_date").cast("timestamp"))
                              + minutes_of_day(F.col("crs_dep_hhmm")) * 60).cast("timestamp"))
    .withColumn("sched_dep_utc", F.expr("to_utc_timestamp(_dep_wall, origin_tz)"))
    .withColumn("sched_arr_utc", (epoch(F.col("sched_dep_utc"))
                                  + F.col("crs_elapsed_min") * 60).cast("timestamp"))
    .withColumn("sched_dep_hour_utc", F.date_trunc("hour", F.col("sched_dep_utc")))
    # outcomes as instants: signed delay minutes on top of the schedule
    .withColumn("actual_dep_utc", (epoch(F.col("sched_dep_utc")) + F.col("dep_delay_min") * 60).cast("timestamp"))
    .withColumn("actual_arr_utc", (epoch(F.col("sched_arr_utc")) + F.col("arr_delay_min") * 60).cast("timestamp"))
    # cross-check: derived arrival back in dest local time vs published CRSArrTime
    .withColumn("_arr_local", F.expr("from_utc_timestamp(sched_arr_utc, dest_tz)"))
    .withColumn("_arr_hhmm_derived", F.hour("_arr_local") * 100 + F.minute("_arr_local"))
    .withColumn("arr_time_mismatch", F.col("_arr_hhmm_derived") != (F.col("crs_arr_hhmm") % 2400))
    .withColumn("flight_status",
                F.when(F.col("cancelled") == 1, "cancelled")
                 .when(F.col("diverted") == 1, "diverted")
                 .otherwise("completed"))
    .drop("_dep_wall", "_arr_local")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quality gates (halt the run)

# COMMAND ----------

def valid_hhmm(c):
    return c.isNull() | ((c >= 0) & (c <= 2400) & ((c % 100) < 60))

def gates(df, cast_loss):
    """name -> (violating-row condition, allowed fraction of rows)."""
    lo, hi = ARR_DELAY_BOUNDS_MIN
    done = F.col("flight_status") == "completed"
    conds = {
        "null_key": (F.greatest(*[F.col(k).isNull().cast("int") for k in KEY]) == 1, 0),
        "non_positive_distance": (F.col("distance_miles").isNull() | (F.col("distance_miles") <= 0), 0),
        "invalid_hhmm": (~(valid_hhmm(F.col("crs_dep_hhmm")) & valid_hhmm(F.col("crs_arr_hhmm"))), 0),
        "airport_without_timezone": (F.col("origin_tz").isNull() | F.col("dest_tz").isNull(), 0),
        "null_scheduled_duration": (F.col("crs_elapsed_min").isNull(), ELAPSED_NULL_TOL),
        "arrival_not_after_departure": (F.col("sched_arr_utc") <= F.col("sched_dep_utc"), 0),
        "completed_missing_arrival_delay": (done & F.col("arr_delay_min").isNull(), 0),
        "arr_delay_out_of_bounds": (done & ((F.col("arr_delay_min") < lo) | (F.col("arr_delay_min") > hi)), 0),
        "dest_local_arrival_mismatch": (F.col("arr_time_mismatch"), ARR_TIME_MISMATCH_TOL),
    }
    return conds

def run_gates(df, cast_loss, label="silver"):
    total = df.count()
    conds = gates(df, cast_loss)
    bad = df.agg(*[F.sum(F.when(c, 1).otherwise(0)).alias(n) for n, (c, _) in conds.items()]).first().asDict()
    key_dupes = total - df.select(*KEY).distinct().count()
    results = [(n, bad[n] or 0, tol) for n, (_, tol) in conds.items()]
    results.append(("duplicate_grain_key", key_dupes, 0))
    results += [(f"cast_loss:{k}", v, 0) for k, v in cast_loss.items() if v]
    failed = [(n, b, tol) for n, b, tol in results if b > tol * total]
    print(f"[{label}] {total:,} rows")
    for n, b, tol in results:
        print(f"  {'FAIL' if (n, b, tol) in failed else 'ok  '} {n}: {b:,} (tolerance {tol:.3%})")
    if failed:
        raise AssertionError(f"Quality gates failed: {[f[0] for f in failed]}")

silver = silver.cache()
run_gates(silver, cast_loss)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prove the gates fire
# MAGIC A gate that has never failed is untested. Corrupt a copy of the data and
# MAGIC confirm the run is halted.

# COMMAND ----------

corrupted = silver.limit(1000).withColumn(
    "distance_miles", F.when(F.col("origin") == F.col("origin"), F.lit(-5.0)))
try:
    run_gates(corrupted, {}, label="deliberately corrupted")
    raise RuntimeError("GATES DID NOT FIRE - the quality gate is broken")
except AssertionError as e:
    print("Gate fired as intended:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write
# MAGIC Cancelled and diverted flights are split out (see `docs/decisions.md`):
# MAGIC they have no valid arrival delay, so mixing them in would bias OTP.

# COMMAND ----------

completed = silver.filter("flight_status = 'completed'")
disrupted = silver.filter("flight_status != 'completed'")

for df, name in ((completed, "flights_completed"), (disrupted, "flights_disrupted")):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{name}"))

print("completed:", spark.table(f"{SILVER}.flights_completed").count())
print("disrupted:", spark.table(f"{SILVER}.flights_disrupted").count())
display(spark.table(f"{SILVER}.flights_disrupted").groupBy("flight_status").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint: trace a real flight by hand
# MAGIC Pick a flight from a published schedule/flight tracker, set the values,
# MAGIC and compare `sched_dep_utc` with the true UTC departure. Choose one that
# MAGIC crosses time zones, and one that crosses midnight.

# COMMAND ----------

TRACE = dict(origin="LAX", dest="JFK")   # edit
display(
    spark.table(f"{BRONZE}.flights_raw")
    .filter((F.col("Origin") == TRACE["origin"]) & (F.col("Dest") == TRACE["dest"]))
    .select("FlightDate", "Marketing_Airline_Network", "Flight_Number_Marketing_Airline",
            "CRSDepTime", "CRSArrTime", "CRSElapsedTime").limit(5))
display(
    spark.table(f"{SILVER}.flights_completed")
    .filter((F.col("origin") == TRACE["origin"]) & (F.col("dest") == TRACE["dest"]))
    .select("flight_date", "marketing_carrier", "flight_number", "crs_dep_hhmm", "origin_tz",
            "sched_dep_utc", "crs_arr_hhmm", "dest_tz", "sched_arr_utc", "arr_time_mismatch").limit(5))
