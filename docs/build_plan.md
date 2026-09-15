# Build plan

Phased plan for building the pipeline. Each phase ends at a checkpoint
that produces something verifiable, so progress is never "half a
notebook".

Tick items off as they are completed. Keep the dates — a visible build
log is more convincing than a repo that appears fully formed overnight.

---

## Phase 0 — Environment

**Goal:** a working Databricks workspace with the source data reachable.

- [ ] Sign up for Databricks Free Edition at `databricks.com/try-databricks`
      (serverless, no cloud account needed; replaced Community Edition,
      retired end of 2025)
- [ ] Complete the built-in "Query and visualise data from a notebook"
      quickstart — 20 minutes, and it teaches the workspace UI
- [ ] Create a catalog and schema in Unity Catalog: `flight_ops`, with
      schemas `bronze`, `silver`, `gold`
- [ ] Create a Volume for raw file landing
- [ ] Download one month of BTS data manually and upload to the Volume —
      confirm you can read it into a DataFrame

**Checkpoint:** `spark.read.csv(...)` returns a DataFrame with the
expected row count for one month.

**Watch out:** Free Edition is serverless and quota-limited. Check the
current limitations page before planning a 12-month backfill; start with
3 months and extend only if quota allows.

---

## Phase 1 — Bronze

**Goal:** raw data landed in Delta, append-only, with provenance.

- [ ] `01_bronze_flights` — read BTS CSVs from the Volume, write to
      `bronze.flights_raw` as Delta
- [ ] Add ingestion metadata columns: `_source_file`, `_ingested_at`
- [ ] Do **not** rename, retype, or filter anything. Bronze mirrors the
      source.
- [ ] `02_bronze_weather` — fetch Open-Meteo hourly archive per airport,
      batched across the full date range in one call per airport, not one
      per day
- [ ] Cache aggressively: check what is already in bronze before calling
      the API. The 10,000/day limit is easy to burn through by accident.
- [ ] Write raw JSON responses to `bronze.weather_raw`

**Checkpoint:** re-running both notebooks does not duplicate rows and
does not re-call the API for data already held.

**Design note to write down:** why append-only, and what you would do
differently if the source issued restatements.

---

## Phase 2 — Silver: flights

**Goal:** clean, typed, deduplicated flight records with correct
timestamps.

- [ ] Type all columns explicitly. Do not rely on schema inference.
- [ ] Build an airport dimension with lat/lon **and IANA timezone**
- [ ] Convert `HHMM` local-time integers to proper timestamps, then to
      UTC. Handle DST.
- [ ] Handle overnight flights — arrival date is not always departure date
- [ ] Separate cancelled and diverted flights into their own table or
      flag them explicitly. Document the decision about whether they
      enter OTP calculations.
- [ ] Deduplicate; confirm the grain is one row per flight
- [ ] Add quality expectations: no null keys, no negative distances,
      arrival after departure, delay values within plausible bounds

**Checkpoint:** a known real flight can be traced from bronze to silver
and its UTC timestamps verified by hand against a published schedule.

**This is the hardest phase.** Timezone handling is where a plausible,
silent, wrong answer is most likely. Budget more time than feels
reasonable.

---

## Phase 3 — Silver: weather join

**Goal:** each flight carries the weather at its origin airport at its
scheduled departure hour.

- [ ] Parse the raw weather JSON into a tabular hourly table
- [ ] Join on `origin_airport` + `scheduled_departure_hour_utc`
- [ ] Decide and document the tolerance: exact hour match, or nearest
      within N minutes
- [ ] Measure and record the join hit rate. A silent 30% miss rate would
      quietly gut the model.
- [ ] Decide how missing weather is handled — imputed, or the row excluded

**Checkpoint:** join hit rate measured and documented; unmatched rows
investigated rather than dropped silently.

---

## Phase 4 — Gold: reporting

**Goal:** aggregate tables a dashboard can read directly.

- [ ] `fct_flight_performance` — one row per flight, denormalised
- [ ] `agg_route_otp` — on-time rate, median delay, volume by route
- [ ] `agg_carrier_otp` — by carrier and month
- [ ] `agg_airport_otp` — by airport, split departures and arrivals
- [ ] Define "on time" explicitly in config (BTS convention: arrival
      within 15 minutes of schedule) and document it as a choice

**Checkpoint:** a headline figure — for example national on-time rate for
a given month — matches the published BTS figure for that month. If it
does not, something upstream is wrong, and this is how you find out.

That reconciliation against an external published number is the single
most valuable check in the whole project. Do not skip it.

---

## Phase 5 — Gold: model features

**Goal:** a feature table containing only pre-departure information.

- [ ] Write the feature list as an explicit **allow-list**, not a
      drop-list of excluded columns
- [ ] Schedule features: day of week, month, scheduled hour, distance,
      carrier, origin, destination
- [ ] Weather features at origin for scheduled departure hour
- [ ] Historical route/carrier delay rates computed on a **trailing
      window that excludes the current flight**
- [ ] Write an assertion that fails if any known post-departure column
      (`DepDelay`, `TaxiOut`, `WheelsOff`, the five cause columns)
      appears in the feature table

**Checkpoint:** the leakage assertion passes, and you can explain to
someone else why each excluded column would have leaked.

---

## Phase 6 — Model

**Goal:** an honestly evaluated delay classifier.

- [ ] Chronological train/test split. Never random — a random split
      trains on the future.
- [ ] Baseline first: predict the trailing historical delay rate for the
      route. Everything else is measured against this.
- [ ] Model: logistic regression or gradient boosting on the feature table
- [ ] Evaluate with precision/recall and AUC-PR, **not accuracy** — most
      flights are on time, so a trivial model scores ~80%
- [ ] Record the confusion matrix and discuss the operational trade-off:
      a false "delayed" prediction has a different cost from a missed one
- [ ] Log runs with MLflow (built into Databricks)

**Checkpoint:** the model beats the trailing-rate baseline on AUC-PR, and
you can state by how much and where it fails.

---

## Phase 7 — Reporting and write-up

- [ ] Power BI dashboard, or Databricks SQL dashboard if simpler
- [ ] `docs/decisions.md` — every judgement call and why
- [ ] Update the README: remove the design-stage banner, add real results
      including the ones that disappointed
- [ ] Only now add the project to the CV

---

## Rough effort

Assuming evenings and weekends alongside other work:

| Phase | Realistic time |
|---|---|
| 0 — Environment | half a day |
| 1 — Bronze | 1–2 days |
| 2 — Silver flights | 3–4 days (timezones) |
| 3 — Weather join | 2 days |
| 4 — Gold reporting | 1–2 days |
| 5 — Features | 1–2 days |
| 6 — Model | 2–3 days |
| 7 — Write-up | 1–2 days |

Roughly three to four weeks part-time. A version that merely runs can be
done much faster; a version that is correct, and that you can defend
line by line in an interview, cannot.

---

## Progress log

Add an entry each session — what was built, what broke, what was decided.
This log is worth as much as the code when someone asks how you work.

| Date | Phase | What happened |
|---|---|---|
| | | |
