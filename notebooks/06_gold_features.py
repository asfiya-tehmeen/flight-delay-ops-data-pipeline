# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Gold: pre-departure feature table
# MAGIC `silver.flights_wx` -> `gold.feat_delay_model`.
# MAGIC
# MAGIC Population: **completed** flights (cancelled/diverted have no arrival delay
# MAGIC to predict). Flights with no origin weather are excluded from this table
# MAGIC (count printed); silver keeps them.
# MAGIC
# MAGIC **Features are an explicit ALLOW-LIST.** A new column in silver cannot
# MAGIC reach the model by default. A second assertion checks the finished table
# MAGIC against a list of known post-departure columns.
# MAGIC
# MAGIC **Trailing history without leakage.** For a flight departing on UTC date D
# MAGIC the history uses only flights whose *actual arrival* date is in
# MAGIC [D-30, D-1]. Keying on arrival date (not scheduled date) matters: a flight
# MAGIC scheduled yesterday evening may land after today's flight has left, so its
# MAGIC outcome is not yet known. Excluding the whole current day also excludes
# MAGIC the flight being predicted.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

ID_COLS = ["flight_date", "marketing_carrier", "flight_number", "origin", "dest",
           "sched_dep_utc", "dep_date_utc"]
LABEL = "label_arr_delayed15"

SCHEDULE_FEATURES = ["month", "day_of_week", "sched_dep_hour_local", "distance_miles", "crs_elapsed_min"]
WEATHER_FEATURES = [f"wx_{v}" for v in WEATHER_VARS]
HISTORY_FEATURES = [f"hist_{k}_{m}" for k in ("route", "origin", "carrier") for m in ("rate", "n")]

# the allow-list: everything the model may see, and nothing else
ALLOW_LIST = SCHEDULE_FEATURES + WEATHER_FEATURES + HISTORY_FEATURES + ["marketing_carrier", "origin", "dest"]

# Known post-departure / outcome columns. Why each leaks: see docs/decisions.md
FORBIDDEN = {
    "dep_hhmm", "dep_delay_min", "dep_del15", "taxi_out_min", "wheels_off_hhmm",
    "wheels_on_hhmm", "taxi_in_min", "arr_hhmm", "arr_delay_min", "arr_del15",
    "actual_elapsed_min", "air_time_min", "carrier_delay_min", "weather_delay_min",
    "nas_delay_min", "security_delay_min", "late_aircraft_delay_min",
    "cancelled", "cancellation_code", "diverted", "div_airport_landings",
    "actual_dep_utc", "actual_arr_utc", "flight_status", "is_on_time", "is_dep_on_time",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Population

# COMMAND ----------

silver = spark.table(f"{SILVER}.flights_wx")
completed = silver.filter("flight_status = 'completed'")

n_completed = completed.count()
no_wx = completed.filter("NOT wx_matched").count()
print(f"completed flights: {n_completed:,} | no origin weather (excluded here): {no_wx:,}")

target = (completed.filter("wx_matched")
          .withColumn("dep_date_utc", F.to_date("sched_dep_utc"))
          .withColumn("sched_dep_hour_local", F.floor(F.col("crs_dep_hhmm") / 100) % 24)
          .withColumn(LABEL, (F.col("arr_delay_min") >= ON_TIME_THRESHOLD_MIN).cast("int")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trailing history (outcome known before departure)

# COMMAND ----------

hist_src = (completed
            .withColumn("arr_date_utc", F.to_date("actual_arr_utc"))
            .withColumn("_delayed", (F.col("arr_delay_min") >= ON_TIME_THRESHOLD_MIN).cast("int"))
            .withColumn("route", F.concat_ws("-", "origin", "dest")))
target = target.withColumn("route", F.concat_ws("-", "origin", "dest"))

def trailing(df, keys, name):
    """Add hist_<name>_rate / hist_<name>_n from arrivals in [D-W, D-1]."""
    daily = hist_src.groupBy(*keys, "arr_date_utc").agg(
        F.count("*").alias("_n"), F.sum("_delayed").alias("_d"))
    cal = df.select(*keys, "dep_date_utc").distinct()
    agg = (cal.join(daily, keys)
           .filter((F.col("arr_date_utc") >= F.date_sub("dep_date_utc", HIST_WINDOW_DAYS))
                   & (F.col("arr_date_utc") <= F.date_sub("dep_date_utc", 1)))
           .groupBy(*keys, "dep_date_utc")
           .agg(F.sum("_n").alias(f"hist_{name}_n"), F.sum("_d").alias("_dsum")))
    agg = agg.withColumn(
        f"hist_{name}_rate",
        F.when(F.col(f"hist_{name}_n") >= HIST_MIN_N, F.col("_dsum") / F.col(f"hist_{name}_n"))
    ).drop("_dsum")
    out = df.join(agg, keys + ["dep_date_utc"], "left")
    return out.withColumn(f"hist_{name}_n", F.coalesce(F.col(f"hist_{name}_n"), F.lit(0)))

feat = trailing(target, ["route"], "route")
feat = trailing(feat, ["origin"], "origin")
feat = trailing(feat, ["marketing_carrier"], "carrier")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select by allow-list, assert, write

# COMMAND ----------

# carrier/origin/dest are both ids and features; select each column once
table = feat.select(*dict.fromkeys(ID_COLS + ALLOW_LIST + [LABEL]))

def assert_no_leakage(columns):
    allowed = set(ID_COLS) | set(ALLOW_LIST) | {LABEL}
    leaked = sorted(set(columns) & FORBIDDEN)
    unknown = sorted(set(columns) - allowed)
    assert not leaked, f"post-departure columns in feature table: {leaked}"
    assert not unknown, f"columns outside the allow-list: {unknown}"

assert_no_leakage(table.columns)
print("leakage assertion passed on", len(table.columns), "columns")

# prove the assertion fires, both ways
for bad in ("dep_delay_min", "some_new_bts_column"):
    try:
        assert_no_leakage(table.columns + [bad])
        raise RuntimeError(f"leakage assertion DID NOT FIRE for {bad}")
    except AssertionError as e:
        print("fired as intended:", e)

# COMMAND ----------

# data-quality gates on the feature table
n = table.count()
checks = {
    "null_label": table.filter(F.col(LABEL).isNull()).count(),
    "null_weather": table.filter(" OR ".join(f"{c} IS NULL" for c in WEATHER_FEATURES)).count(),
    "null_schedule": table.filter(" OR ".join(f"{c} IS NULL" for c in SCHEDULE_FEATURES)).count(),
    "rate_out_of_range": table.filter(" OR ".join(
        f"({c} < 0 OR {c} > 1)" for c in HISTORY_FEATURES if c.endswith("_rate"))).count(),
    "duplicate_flights": n - table.select(*ID_COLS[:5], "sched_dep_utc").distinct().count(),
}
print(f"{n:,} rows", checks)
assert all(v == 0 for v in checks.values()), f"feature gates failed: {checks}"

(table.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.feat_delay_model"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks worth reading
# MAGIC * Label prevalence overall and by split (a class-imbalance sanity check).
# MAGIC * History availability: the first days of the month have no history, which
# MAGIC   is an honest limitation of a one-month build.
# MAGIC * Independent recompute of one flight's history with plain filters.

# COMMAND ----------

ft = spark.table(f"{GOLD}.feat_delay_model")
display(ft.groupBy(F.when(F.col("dep_date_utc") <= TRAIN_END, "train")
                    .when(F.col("dep_date_utc") <= VAL_END, "val")
                    .when(F.col("dep_date_utc") >= TEST_START, "test")
                    .otherwise("gap").alias("split"))
          .agg(F.count("*").alias("flights"),
               F.round(F.avg(LABEL), 4).alias("delayed_rate"),
               F.round(F.avg((F.col("hist_route_rate").isNotNull()).cast("int")), 3).alias("has_route_history"))
          .orderBy("split"))

# COMMAND ----------

# independent recompute for one sample flight
s = ft.filter("hist_route_rate IS NOT NULL").orderBy(F.desc("hist_route_n")).limit(1).collect()[0]
route = f"{s['origin']}-{s['dest']}"
d = s["dep_date_utc"]
manual = (hist_src.filter((F.col("route") == route)
                          & (F.col("arr_date_utc") >= F.date_sub(F.lit(d), HIST_WINDOW_DAYS))
                          & (F.col("arr_date_utc") <= F.date_sub(F.lit(d), 1)))
          .agg(F.count("*").alias("n"), F.avg("_delayed").alias("rate")).first())
print(route, d, "| table:", s["hist_route_n"], round(s["hist_route_rate"], 6),
      "| manual:", manual["n"], round(manual["rate"], 6))
assert manual["n"] == s["hist_route_n"] and abs(manual["rate"] - s["hist_route_rate"]) < 1e-9
