# Flight Operations Pipeline — Design Document

> **Status: design stage.**
>
> This repository currently contains the architecture, data source
> research, and build plan for a medallion-architecture pipeline on
> Databricks. It is published as a design document so the reasoning is
> tracked from the start.
>

---

## What this will be

A Databricks pipeline that ingests US domestic flight on-time data and
historical weather, joins them through bronze/silver/gold layers in Delta
Lake, and produces two outputs:

1. **Operational reporting** — on-time performance by route, carrier,
   airport and time of day.
2. **A pre-departure delay model** — predicting arrival delay using only
   information available *before* the aircraft leaves the gate.

The second constraint is the point of the project. See "Design
principles" below.

## Why Databricks rather than the local stack

The companion project in this portfolio
([retail demand forecasting](https://github.com/asfiya-tehmeen/retail-demand-pipeline))
runs on DuckDB, which is the right tool for 913,000 rows on a laptop.
This dataset is different: US domestic flights run roughly 620,000–665,000
per month, so a single year is around 7.5 million rows before the weather
join fans it out further. That is a genuine reason to reach for Spark
rather than a stylistic one.

The pipeline is built on Databricks Free Edition — a serverless,
quota-limited workspace that replaced the retired Community Edition in
2025. No cloud account or credit card required.

---

## Architecture

```
  SOURCES
    BTS TranStats           Open-Meteo Archive API
    (monthly CSV/ZIP)       (hourly ERA5, per airport)
         |                          |
         v                          v
  +----------------------------------------------+
  |  BRONZE   raw landing, Delta, append-only    |
  |  - source files as delivered, no reshaping   |
  |  - ingestion metadata: file name, load time  |
  |  - schema-on-read, nothing rejected          |
  +----------------------------------------------+
         |
         v
  +----------------------------------------------+
  |  SILVER   cleaned + conformed + joined       |
  |  - typed, deduplicated, canonical names      |
  |  - cancelled/diverted flights separated      |
  |  - weather joined at origin airport + hour   |
  |  - data quality expectations enforced        |
  +----------------------------------------------+
         |
         v
  +----------------------------------------------+
  |  GOLD     business aggregates + ML features  |
  |  - fct_flight_performance (per flight)       |
  |  - agg_route_otp / agg_carrier_otp           |
  |  - feat_delay_model (pre-departure only)     |
  +----------------------------------------------+
         |
         v
     Power BI  +  delay prediction model
```

### Why the layers are separated

Bronze holds the source exactly as delivered. When a number looks wrong
in a dashboard three months from now, bronze is what lets you prove
whether the pipeline broke it or it arrived that way — without re-pulling
from the source, which for an API with rate limits may not be possible.

Silver is where every transformation that could be *wrong* happens:
typing, deduplication, the weather join. Keeping it separate from gold
means aggregation logic can be rewritten without re-doing the cleaning.

Gold is shaped for consumption. Nothing downstream reads silver directly.

---

## Design principles carried over from the retail project

### Features must be knowable at prediction time

This is the same discipline that governed the retail pipeline, and it
matters more here because the BTS schema is full of traps.

The dataset contains `DepDelay`, `TaxiOut`, `WheelsOff`, and five
delay-cause columns (`CarrierDelay`, `WeatherDelay`, `NASDelay`,
`SecurityDelay`, `LateAircraftDelay`). Every one of those is recorded
*after* the flight has departed or landed. A model predicting arrival
delay using `DepDelay` will score spectacularly and be worthless: if you
already know the departure delay, you no longer need a pre-departure
prediction.

The model will therefore use only:

- Schedule features — scheduled departure/arrival, day of week, month,
  distance, carrier, origin, destination
- Weather at the origin airport for the scheduled departure hour
- Historical aggregates computed from **prior** periods only — e.g. this
  route's delay rate over the previous 30 days, never including the
  flight being predicted

Post-departure columns will be kept in silver for *reporting*, and
excluded from the feature table by an explicit allow-list rather than a
drop-list, so that a new BTS column cannot leak in by default.

### Quality gates halt the run

Each layer transition asserts its contract and stops the pipeline on
failure, rather than passing bad rows downstream where they reach a
dashboard and get trusted.

### Assumptions are isolated and labelled

Anything not observed in the data — join tolerances, delay thresholds,
the 15-minute "on-time" definition — lives in one config location and is
documented as an assumption.

---

## Known challenges to solve during the build

Recorded up front so they are not discovered late:

**Timezones.** BTS records times in *local time at each airport* with no
offset, as an integer `HHMM`. Joining to weather (UTC) requires mapping
every airport to its timezone and handling DST transitions. This is the
hardest correctness problem in the project and the most likely source of
a silent, plausible-looking error.

**Overnight flights.** A departure at 2330 arriving 0615 crosses
midnight. Naive arithmetic on `HHMM` integers gives a negative duration.

**Cancelled and diverted flights.** These have null arrival times.
Including them in an average delay silently biases it downward, because
a cancelled flight is arguably the worst outcome and is recorded as no
delay at all. They need separate handling and a documented decision.

**Weather API volume.** Open-Meteo allows 10,000 free calls/day for
non-commercial use. Fetching hourly weather per airport per day naively
will exceed that. The fetch must be batched by airport across long date
ranges, cached in bronze, and never re-requested.

**Class imbalance.** Most flights are on time. A classifier predicting
"never delayed" will score around 80% accuracy and be useless. Evaluation
needs precision/recall or AUC-PR against a sensible baseline, not
accuracy.

---

## Data sources

| Source | What | Access |
|---|---|---|
| [BTS TranStats](https://transtats.bts.gov/) Marketing Carrier On-Time Performance | ~620–665K US domestic flights/month, 2018–present | Free, public, monthly CSV/ZIP, no key |
| [Open-Meteo Archive API](https://open-meteo.com/) | Hourly historical weather (ERA5) by lat/lon, 1940–present | Free, no API key, 10,000 calls/day non-commercial, **CC BY 4.0 — attribution required** |
| OpenFlights / BTS master coordinate list | Airport lat/lon and timezone, needed for the weather join | Free, public |

---

## Planned repository layout

```
notebooks/
  00_setup                 catalog, schema, volume creation
  01_bronze_flights        BTS ingest -> Delta, append-only
  02_bronze_weather        Open-Meteo batched fetch -> Delta
  03_silver_flights        typing, dedup, timezone resolution
  04_silver_weather_join   weather joined at origin + scheduled hour
  05_gold_aggregates       route / carrier / airport OTP tables
  06_gold_features         pre-departure feature table (allow-list)
  07_model_delay           baseline + model, AUC-PR evaluation
docs/
  build_plan.md            phased plan and progress log
  resources.md             setup guide, data dictionaries, references
  decisions.md             design decisions and why (to be written during build)
```

---

## Licence and attribution

Weather data © Open-Meteo, licensed CC BY 4.0.
Flight data: US Department of Transportation, Bureau of Transportation
Statistics — public domain.
