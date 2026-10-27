# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver: weather join
# MAGIC 1. Parse `bronze.weather_raw` JSON -> `silver.weather_hourly` (one row per
# MAGIC    airport per UTC hour).
# MAGIC 2. Join to flights on **origin airport + scheduled departure hour (UTC)**
# MAGIC    -> `silver.flights_wx` (all flight statuses, with `flight_status`).
# MAGIC
# MAGIC **Join rule:** exact match on the hour *containing* the scheduled departure
# MAGIC (truncate, never round). Rounding up could attach weather observed AFTER
# MAGIC the planned departure, which a pre-departure model could not know.
# MAGIC
# MAGIC **Missing weather:** rows are kept with `wx_matched = false`, never dropped.
# MAGIC The feature stage decides whether to impute or exclude.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parse the raw JSON

# COMMAND ----------

hourly_fields = ", ".join(f"{v}:array<double>" for v in WEATHER_VARS)
schema = f"hourly struct<time:array<string>, {hourly_fields}>"

raw = (spark.table(f"{BRONZE}.weather_raw")
       .filter((F.col("start_date") == WEATHER_START_DATE) & (F.col("end_date") == WEATHER_END_DATE))
       .select("iata", F.from_json("raw_json", schema).alias("j")))

zipped = F.arrays_zip(
    F.col("j.hourly.time").alias("time"),
    *[F.col(f"j.hourly.{v}").alias(v) for v in WEATHER_VARS])

weather = (raw.select("iata", F.explode(zipped).alias("h"))
           .select("iata",
                   F.to_timestamp("h.time", "yyyy-MM-dd'T'HH:mm").alias("hour_utc"),
                   *[F.col(f"h.{v}").alias(f"wx_{v}") for v in WEATHER_VARS]))

# COMMAND ----------

# Gates on the weather table itself
n = weather.count()
dupes = n - weather.select("iata", "hour_utc").distinct().count()
bad_ts = weather.filter("hour_utc IS NULL").count()
per_airport = weather.groupBy("iata").count()
wrong_len = per_airport.filter(F.col("count") != WEATHER_EXPECTED_HOURS).count()
print(f"{n:,} rows, {per_airport.count()} airports, expected {WEATHER_EXPECTED_HOURS} hours each")
print(f"duplicate (airport, hour): {dupes} | unparseable hour: {bad_ts} | airports with wrong hour count: {wrong_len}")

nulls = weather.agg(*[F.round(100 * F.avg(F.col(c).isNull().cast("int")), 3).alias(c)
                      for c in weather.columns if c.startswith("wx_")]).first().asDict()
print("null % per variable:", nulls)   # informational: some variables may be sparse

assert dupes == 0 and bad_ts == 0 and wrong_len == 0, "weather table failed its contract"
assert nulls["wx_temperature_2m"] == 0, "temperature should be complete (ERA5)"

(weather.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.weather_hourly"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Join

# COMMAND ----------

flights = (spark.table(f"{SILVER}.flights_completed")
           .unionByName(spark.table(f"{SILVER}.flights_disrupted")))
wx = spark.table(f"{SILVER}.weather_hourly").withColumn("_matched", F.lit(True))

joined = (flights.join(F.broadcast(wx),
                       (flights.origin == wx.iata) & (flights.sched_dep_hour_utc == wx.hour_utc),
                       "left")
          .withColumn("wx_matched", F.coalesce(F.col("_matched"), F.lit(False)))
          .drop("iata", "hour_utc", "_matched"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hit rate (the checkpoint)

# COMMAND ----------

joined = joined.cache()
n_flights, n_joined = flights.count(), joined.count()
assert n_flights == n_joined, f"join changed row count {n_flights} -> {n_joined} (fan-out)"

hit = joined.agg(F.avg(F.col("wx_matched").cast("int"))).first()[0]
print(f"rows {n_joined:,} | weather hit rate {hit:.4%}")
display(joined.groupBy("flight_status", "wx_matched").count().orderBy("flight_status", "wx_matched"))

# unmatched rows are investigated, not dropped silently
unmatched = joined.filter("NOT wx_matched")
display(unmatched.groupBy("origin").count().orderBy(F.desc("count")).limit(15))
display(unmatched.groupBy(F.to_date("sched_dep_utc").alias("utc_date")).count().orderBy("utc_date"))

assert hit >= WEATHER_HIT_RATE_MIN, f"weather hit rate {hit:.4%} below {WEATHER_HIT_RATE_MIN:.2%}"

# COMMAND ----------

(joined.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.flights_wx"))
print(spark.table(f"{SILVER}.flights_wx").count(), "rows in silver.flights_wx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot check
# MAGIC Pick a stormy airport-day and confirm the numbers look like weather.

# COMMAND ----------

display(spark.table(f"{SILVER}.flights_wx")
        .filter("wx_matched")
        .orderBy(F.desc("wx_snowfall")).limit(10)
        .select("flight_date", "origin", "sched_dep_utc", "sched_dep_hour_utc",
                "wx_temperature_2m", "wx_snowfall", "wx_wind_gusts_10m", "wx_visibility",
                "flight_status", "arr_delay_min"))
