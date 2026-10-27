# Databricks notebook source
# MAGIC %md
# MAGIC # Shared config
# MAGIC Every assumption that is not observed in the data lives here.
# MAGIC Pulled into other notebooks with `%run ./config`.

# COMMAND ----------

CATALOG = "flight_ops"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

VOLUME_ROOT = f"/Volumes/{CATALOG}/bronze/landing"
FLIGHTS_DIR = f"{VOLUME_ROOT}/bts"          # BTS monthly ZIP/CSV files
REFERENCE_DIR = f"{VOLUME_ROOT}/reference"  # OpenFlights airports.dat

# Scope of the build. Free Edition is quota-limited: start with one month.
START_DATE = "2024-01-01"
END_DATE = "2024-01-31"

# ASSUMPTION: BTS convention, a flight is on time if it arrives < 15 min late.
ON_TIME_THRESHOLD_MIN = 15

# Open-Meteo archive API (CC BY 4.0, attribution required).
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_VARS = [
    "temperature_2m", "precipitation", "snowfall", "wind_speed_10m",
    "wind_gusts_10m", "visibility", "cloud_cover",
]
WEATHER_BATCH_SIZE = 10      # locations per request (comma-separated lat/lon)
WEATHER_SLEEP_SEC = 2        # pause between requests, stay well under limits
