# Resources

Everything needed to build this. Gathered up front so the build is not
interrupted by hunting for links.

---

## 1. Databricks Free Edition

**Sign up:** https://www.databricks.com/try-databricks

Free Edition replaced Databricks Community Edition, which was retired at
the end of 2025. It is serverless and quota-limited — no cloud account,
no compute provisioning, no credit card. Personal and learning use only;
the terms do not permit commercial use.

- Setup guide (Azure): https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition
- Setup guide (AWS): https://docs.databricks.com/aws/en/getting-started/free-edition
- **Check the limitations page before planning a large backfill.** Quotas
  apply to compute and storage, so start with 3 months of flight data and
  extend only if headroom allows.

Concepts worth understanding before writing code, because they are what
interviewers ask about:

- **Unity Catalog** — the three-level namespace `catalog.schema.table`.
  This is how modern Databricks organises and governs data.
- **Volumes** — governed storage for raw files, where the BTS CSVs land
  before being read into Delta.
- **Delta Lake** — the table format underneath everything. Know why it
  exists: ACID transactions, time travel, schema enforcement, and the
  ability to update and delete rows, none of which plain Parquet gives
  you. "Why Delta over Parquet" is a near-certain interview question.
- **MLflow** — experiment tracking, built in. Use it in Phase 6.

---

## 2. Flight data — BTS TranStats

**Portal:** https://transtats.bts.gov/

Navigate to Aviation → **Marketing Carrier On-Time Performance (Beginning
January 2018)**. Download by month; each file is a zipped CSV of roughly
620,000–665,000 US domestic flights.

Free, public domain, no account or API key.

### Columns that matter

*Identity and schedule — available before departure, safe as features:*

| Column | Meaning |
|---|---|
| `FlightDate` | Date of the flight |
| `Marketing_Airline_Network` / `Operating_Airline` | Carrier codes — note these differ for codeshares |
| `Origin`, `Dest` | Airport codes |
| `CRSDepTime`, `CRSArrTime` | **Scheduled** times, local, integer `HHMM` |
| `CRSElapsedTime` | Scheduled duration, minutes |
| `Distance` | Miles |

*Outcome — available only after the fact. Use for reporting, never as
model features:*

| Column | Why it leaks |
|---|---|
| `DepTime`, `DepDelay`, `DepDel15` | Recorded at departure |
| `TaxiOut`, `WheelsOff`, `WheelsOn`, `TaxiIn` | During the flight |
| `ArrTime`, `ArrDelay`, `ArrDel15` | The target itself |
| `ActualElapsedTime`, `AirTime` | Post-flight |
| `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay` | Assigned retrospectively, and only populated when arrival delay ≥ 15 min — using them leaks the label twice over |
| `Cancelled`, `Diverted` | Known only on the day |

**The trap:** `DepDelay` correlates enormously with `ArrDelay`. A model
using it will look excellent and be useless, because a pre-departure
forecast that requires the departure delay as input cannot be run before
departure.

### Definitions

- **On time** = arrival within 15 minutes of schedule (BTS convention).
- Times are **local to each airport**, stored as integers. `0930` is
  09:30 local. There is no timezone or offset column — you must supply it.
- Cancelled flights have null arrival times and are excluded from delay
  averages by BTS convention. Decide and document your own handling.

Published monthly summary figures are at
https://www.bts.gov/topics/airlines-and-airports — use these to reconcile
your Phase 4 aggregates. This external check is the most valuable
validation in the project.

---

## 3. Weather — Open-Meteo Archive API

**Docs:** https://open-meteo.com/en/docs/historical-weather-api

- No API key, no sign-up
- ERA5 reanalysis from 1940 to present, hourly, spatially complete with
  no missing values
- Free for non-commercial use up to **10,000 API calls per day**
- Licensed **CC BY 4.0 — attribution is required.** Credit Open-Meteo in
  the README.
- Returns JSON by default; CSV and XLSX also available
- **Multiple locations can be queried in one request** via comma-separated
  coordinate lists — use this to stay inside the quota

Example shape of a request (one airport, a long date range, hourly
variables in a single call):

```
https://archive-api.open-meteo.com/v1/archive
  ?latitude=33.64&longitude=-84.43
  &start_date=2024-01-01&end_date=2024-03-31
  &hourly=temperature_2m,precipitation,snowfall,wind_speed_10m,wind_gusts_10m,visibility,cloud_cover
  &timezone=UTC
```

Request in **UTC** and do the timezone work on the flight side. Mixing
local-time weather with local-time flights across different zones is how
subtle join errors get in.

Variables worth pulling for delay prediction: precipitation, snowfall,
wind speed, wind gusts, visibility, cloud cover, temperature.

---

## 4. Airport coordinates and timezones

Needed to join weather to airports and to convert local times to UTC.

- **OpenFlights airports database** — https://openflights.org/data.html —
  includes IATA code, lat/lon, and timezone. Free, widely used.
- Cross-check against the BTS master coordinate lookup on TranStats.

Store this as a proper dimension table in silver. Do not hardcode a
dictionary in a notebook — it is reference data, it will need correcting,
and it belongs in the warehouse.

**Use IANA timezone names** (`America/New_York`), not fixed UTC offsets.
Offsets change twice a year; IANA names handle DST correctly.

---

## 5. Concepts to read up on before building

Ordered by when you will need them.

**Medallion architecture** — Databricks' own documentation on bronze/
silver/gold. Understand *why* the layers exist, not just what they are
called. The question "why not just transform once?" has a real answer
about reprocessing and provenance.

**Delta Lake fundamentals** — ACID guarantees, time travel, `MERGE`,
schema enforcement and evolution. Specifically be able to answer "why
Delta rather than Parquet".

**Spark execution model** — lazy evaluation, transformations vs actions,
shuffles, partitioning, and why a `groupBy` on a high-cardinality column
is expensive. You do not need deep internals, but "why was this job
slow" is a standard interview question and the answer is usually shuffle.

**Idempotent pipelines** — why re-running a job should not duplicate
data, and how `MERGE` and append-only design achieve that.

**Class imbalance** — precision, recall, AUC-PR, and why accuracy is the
wrong metric when 80% of the label is one class.

**Time-based validation** — why a random train/test split leaks the
future, the same principle that governed the retail project.

---

## 6. Cost and quota discipline

Free Edition is quota-limited, so treat compute as scarce:

- Develop on a **sampled subset** — one month, or a handful of airports.
  Run the full set only once the logic is settled.
- Cache the weather API responses in bronze and never re-fetch. Burning
  the daily quota costs you a day.
- Write intermediate results to Delta rather than recomputing a long
  chain every time a notebook restarts.
- Do not leave notebooks attached to running compute.

---

## 7. What "done" looks like

The project is finished when:

- The pipeline runs end to end from raw files to gold tables
- Quality gates halt the run on bad data, and you have tested that they
  actually fire
- A headline aggregate reconciles against the published BTS figure
- The leakage assertion passes and you can explain every excluded column
- The model beats a trailing-rate baseline on AUC-PR, and you know where
  it fails
- `docs/decisions.md` records the judgement calls
- You can walk someone through any notebook without notes

Only at that point does it go on the CV.
