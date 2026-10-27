# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold: reporting
# MAGIC Reads `silver.flights_wx`; writes
# MAGIC * `gold.fct_flight_performance` - one row per flight (all statuses)
# MAGIC * `gold.agg_route_otp`, `gold.agg_carrier_otp`, `gold.agg_airport_otp`,
# MAGIC   `gold.agg_time_block_otp`
# MAGIC
# MAGIC **On time** = arrival delay < `ON_TIME_THRESHOLD_MIN` (BTS: a flight 15+
# MAGIC minutes late is delayed). Delay metrics use *completed* flights only;
# MAGIC cancellation and diversion are reported as separate rates and are never
# MAGIC folded into a delay average (see `docs/decisions.md`).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F

THR = ON_TIME_THRESHOLD_MIN
silver = spark.table(f"{SILVER}.flights_wx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact table

# COMMAND ----------

completed = F.col("flight_status") == "completed"
dep_hour_local = (F.floor(F.col("crs_dep_hhmm") / 100) % 24)

fct = (
    silver
    .withColumn("route", F.concat_ws("-", "origin", "dest"))
    .withColumn("sched_dep_hour_local", dep_hour_local)
    .withColumn("dep_time_block",
                F.when(dep_hour_local < 6, "night (00-05)")
                 .when(dep_hour_local < 12, "morning (06-11)")
                 .when(dep_hour_local < 18, "afternoon (12-17)")
                 .otherwise("evening (18-23)"))
    # NULL (not false) when the flight did not operate: absence is not lateness
    .withColumn("is_on_time", F.when(completed, F.col("arr_delay_min") < THR))
    .withColumn("is_dep_on_time", F.when(F.col("dep_delay_min").isNotNull(), F.col("dep_delay_min") < THR))
    .drop("bts_duplicate_flag", "arr_time_mismatch")
)

(fct.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.fct_flight_performance"))
fct = spark.table(f"{GOLD}.fct_flight_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregates

# COMMAND ----------

def otp(group_cols):
    """Per-group OTP metrics. Delay stats on completed flights only."""
    delay = F.when(completed, F.col("arr_delay_min"))
    g = fct.groupBy(*group_cols).agg(
        F.count("*").alias("flights_scheduled"),
        F.sum(completed.cast("int")).alias("flights_completed"),
        F.sum((F.col("flight_status") == "cancelled").cast("int")).alias("flights_cancelled"),
        F.sum((F.col("flight_status") == "diverted").cast("int")).alias("flights_diverted"),
        F.sum(F.col("is_on_time").cast("int")).alias("flights_on_time"),
        F.round(F.avg(delay), 2).alias("avg_arr_delay_min"),
        F.percentile_approx(delay, 0.5).alias("median_arr_delay_min"),
    )
    return (g.withColumn("on_time_rate", F.round(F.col("flights_on_time") / F.col("flights_completed"), 4))
             .withColumn("cancel_rate", F.round(F.col("flights_cancelled") / F.col("flights_scheduled"), 4))
             .withColumn("diversion_rate", F.round(F.col("flights_diverted") / F.col("flights_scheduled"), 4)))

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{name}")

save(otp(["origin", "dest", "year", "month"]), "agg_route_otp")
save(otp(["marketing_carrier", "year", "month"]), "agg_carrier_otp")
save(otp(["dep_time_block", "year", "month"]), "agg_time_block_otp")

# Airports: departures and arrivals are different populations, kept separate.
def airport_side(role, code_col, delay_col, operated):
    d = F.when(operated, F.col(delay_col))
    on_time = F.when(operated, F.col(delay_col) < THR)
    return (fct.groupBy(F.col(code_col).alias("airport"), "year", "month").agg(
                F.count("*").alias("flights_scheduled"),
                F.sum(operated.cast("int")).alias("flights_operated"),
                F.sum((F.col("flight_status") == "cancelled").cast("int")).alias("flights_cancelled"),
                F.sum(on_time.cast("int")).alias("flights_on_time"),
                F.round(F.avg(d), 2).alias("avg_delay_min"),
                F.percentile_approx(d, 0.5).alias("median_delay_min"))
            .withColumn("role", F.lit(role))
            .withColumn("on_time_rate", F.round(F.col("flights_on_time") / F.col("flights_operated"), 4)))

departures = airport_side("departure", "origin", "dep_delay_min", F.col("dep_delay_min").isNotNull())
arrivals = airport_side("arrival", "dest", "arr_delay_min", completed)
save(departures.unionByName(arrivals), "agg_airport_otp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gates

# COMMAND ----------

n_silver, n_fct = silver.count(), fct.count()
assert n_silver == n_fct, f"fact rows {n_fct} != silver rows {n_silver}"

for t in ("agg_route_otp", "agg_carrier_otp", "agg_time_block_otp"):
    s = spark.table(f"{GOLD}.{t}").agg(F.sum("flights_scheduled")).first()[0]
    assert s == n_fct, f"{t}: sums to {s}, expected {n_fct}"

for t in ("agg_route_otp", "agg_carrier_otp", "agg_airport_otp", "agg_time_block_otp"):
    bad = spark.table(f"{GOLD}.{t}").filter(
        (F.col("on_time_rate") < 0) | (F.col("on_time_rate") > 1)).count()
    assert bad == 0, f"{t}: {bad} rows with on_time_rate outside [0,1]"
print("gold gates passed;", n_fct, "fact rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation against published BTS figures (the key check)
# MAGIC 1. Look up the published national figures for the month on bts.gov
# MAGIC    (https://www.bts.gov/topics/airlines-and-airports), plus the exact
# MAGIC    definition and which carriers they cover.
# MAGIC 2. Enter them below. Both candidate definitions are computed because the
# MAGIC    denominator differs between BTS publications.

# COMMAND ----------

# DOT Air Travel Consumer Report, January 2024 (marketing network carriers):
# 582,425 scheduled domestic flights, 22,073 (3.8%) cancelled, 72.8% on-time arrival.
# Published on-time counts cancelled/diverted as NOT on time (denominator = scheduled).
PUBLISHED = {"on_time_pct": 72.8, "cancelled_pct": 3.8, "scheduled": 582_425, "cancelled": 22_073}
TOLERANCE_PP = 0.1    # published figures are rounded to 0.1

t = fct.agg(
    F.count("*").alias("scheduled"),
    F.sum(completed.cast("int")).alias("completed"),
    F.sum((F.col("flight_status") == "cancelled").cast("int")).alias("cancelled"),
    F.sum((F.col("flight_status") == "diverted").cast("int")).alias("diverted"),
    F.sum(F.col("is_on_time").cast("int")).alias("on_time")).first()

ours = {
    "on_time_pct (of completed flights)": 100 * t.on_time / t.completed,
    "on_time_pct (of all scheduled, cancel/divert count as not on time)": 100 * t.on_time / t.scheduled,
    "cancelled_pct (of scheduled)": 100 * t.cancelled / t.scheduled,
    "diverted_pct (of scheduled)": 100 * t.diverted / t.scheduled,
}
for k, v in ours.items():
    print(f"{k}: {v:.2f}")

if PUBLISHED["on_time_pct"] is None:
    print("\nPUBLISHED not filled in - reconciliation NOT done yet.")
else:
    assert t.scheduled == PUBLISHED["scheduled"], f"scheduled {t.scheduled} != published {PUBLISHED['scheduled']}"
    assert t.cancelled == PUBLISHED["cancelled"], f"cancelled {t.cancelled} != published {PUBLISHED['cancelled']}"
    on_time = ours["on_time_pct (of all scheduled, cancel/divert count as not on time)"]
    assert abs(on_time - PUBLISHED["on_time_pct"]) <= TOLERANCE_PP, \
        f"on-time {on_time:.2f} vs published {PUBLISHED['on_time_pct']} - investigate upstream"
    assert abs(ours["cancelled_pct (of scheduled)"] - PUBLISHED["cancelled_pct"]) <= TOLERANCE_PP
    print("Reconciled: flight counts exact, rates within", TOLERANCE_PP, "pp.")

# COMMAND ----------

display(spark.table(f"{GOLD}.agg_carrier_otp").orderBy(F.desc("flights_scheduled")))
display(spark.table(f"{GOLD}.agg_route_otp").filter("flights_completed >= 200").orderBy("on_time_rate").limit(10))
