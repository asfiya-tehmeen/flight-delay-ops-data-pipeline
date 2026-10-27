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

# Flight dates are LOCAL, weather is UTC. Departures late on the last local day
# fall on the next UTC day (US evenings), and early on the first local day can
# fall on the previous UTC day (Guam is UTC+10). Pad one day each side.
from datetime import date, timedelta
WEATHER_START_DATE = str(date.fromisoformat(START_DATE) - timedelta(days=1))
WEATHER_END_DATE = str(date.fromisoformat(END_DATE) + timedelta(days=1))
WEATHER_EXPECTED_HOURS = (
    (date.fromisoformat(WEATHER_END_DATE) - date.fromisoformat(WEATHER_START_DATE)).days + 1) * 24

# ASSUMPTION: minimum share of flights that must find origin weather.
WEATHER_HIT_RATE_MIN = 0.995

# ASSUMPTION: BTS convention, a flight is on time if it arrives < 15 min late.
ON_TIME_THRESHOLD_MIN = 15

# ASSUMPTION: airports missing from the OpenFlights snapshot, added by hand.
# (lat, lon, IANA tz). Coordinates are approximate - ERA5 weather is a ~25 km
# grid so ~1 km error is immaterial. VERIFY against an authoritative source.
AIRPORT_OVERRIDES = {
    # Williston Basin Intl, ND - opened 2019, replaced Sloulin Field (ISN)
    "XWA": (48.2594, -103.7511, "America/Chicago"),
    # Eastern Sierra Regional, Bishop CA - in OpenFlights but timezone missing
    "BIH": (37.3731, -118.3636, "America/Los_Angeles"),
}

# ASSUMPTION: BTS flags records it considers duplicates in the `Duplicate`
# column. Excluded from silver; the count removed is printed every run.
EXCLUDE_DUPLICATE_FLAG = True

# ASSUMPTIONS: quality-gate tolerances (fraction of rows) and plausible bounds.
ARR_DELAY_BOUNDS_MIN = (-180, 4320)    # outside this -> data error, not a delay
ELAPSED_NULL_TOL = 0.001               # scheduled duration needed to derive arrival
ARR_TIME_MISMATCH_TOL = 0.01           # derived dest-local arrival vs CRSArrTime

# Open-Meteo archive API (CC BY 4.0, attribution required).
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_VARS = [
    "temperature_2m", "precipitation", "snowfall", "wind_speed_10m",
    "wind_gusts_10m", "visibility", "cloud_cover",
]
WEATHER_BATCH_SIZE = 10      # locations per request (comma-separated lat/lon)
WEATHER_SLEEP_SEC = 2        # pause between requests, stay well under limits
