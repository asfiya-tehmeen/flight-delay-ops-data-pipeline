# Design decisions

Every judgement call, and why. Newest at the bottom of each section.

## Bronze

**Append-only, all columns as strings.** Bronze must show what the source
delivered. If BTS restated a month, the restated file would land alongside the
old one (distinguishable by `_source_file`/`_ingested_at`) and silver would
pick the right one; overwriting would destroy the evidence.

**Header whitespace is trimmed, values are not.** BTS emits `Operating_Airline `
with a trailing space; Delta rejects whitespace in column names. This is the
only deviation from "mirror the source".

**Airports missing from OpenFlights are added by hand** in
`AIRPORT_OVERRIDES` (config). First case: `XWA` (Williston Basin Intl, opened
2019). Coordinates approximate; ERA5 is a ~25 km grid so this is immaterial.

## Silver: flights

**Scheduled arrival is derived, not parsed.** `sched_arr_utc = sched_dep_utc +
CRSElapsedTime`. Parsing `CRSArrTime` needs a guess at the arrival date (overnight,
cross-timezone). The derived arrival is converted back to destination local
time and compared with `CRSArrTime`; the mismatch rate is a quality gate.

**Actual times are schedule + signed delay** (`actual_dep_utc = sched_dep_utc +
DepDelay`), because `DepTime`/`ArrTime` carry no date.

**Cancelled and diverted flights are split into `flights_disrupted`.** They have
no valid arrival delay. Putting them in a delay average would either bias it
(treated as 0) or drop them silently. `flights_completed` feeds on-time-rate
and delay metrics (BTS convention); cancellation/diversion rates are reported
separately from `flights_disrupted`. An OTP figure that excludes cancellations
flatters the airline, so dashboards must show both.

**BTS `Duplicate = 'Y'` rows are excluded** (`EXCLUDE_DUPLICATE_FLAG`); the
number removed is printed each run. Identical rows are also dropped; rows that
collide on the grain key but differ are NOT dropped, the grain gate fails.

**Not carried into silver:** Div2-Div5 diversion detail, DOT/SeqID/WAC/FIPS
code columns, delay-group buckets. Not needed for reporting or features; still
in bronze if wanted.

**Arrival-delay bound is 72 hours (4320 min).** The first guess of 2000 min
failed the gate on 16 real flights (Jan 2024 winter storm, max 3136 min / ~52 h),
all American Airlines, with departure delay matching arrival delay within a
few minutes, so genuine. A tighter bound would have silently dropped the
worst real outcomes and biased delay averages down.

**Airports with missing timezone in OpenFlights are overridden by hand**
(`BIH`, Bishop CA, America/Los_Angeles). Override coordinates replace
OpenFlights ones in the dimension; bronze weather for already-fetched airports
used the OpenFlights coordinates (difference immaterial at ERA5 resolution).

**DST:** `to_utc_timestamp` resolves the overlap hour to the earlier offset and
shifts times in the spring gap forward. January data never exercises this; it
must be re-checked before extending the date range across March or November.

## Silver: weather join

**Weather is fetched for the flight range padded by one day each side.**
Flight dates are local, weather is UTC. Late-evening US departures on the last
day are already the next UTC day; early departures from Guam (UTC+10) on the
first day are the previous UTC day. Found by reasoning before the first join,
not by observing misses. Superseded unpadded rows are kept in bronze for
provenance; silver reads only the current range.

**Join = exact match on the hour containing the scheduled departure
(truncate, not round).** Rounding to the nearest hour would attach weather
observed up to 30 minutes after the planned departure, information a
pre-departure model does not have. Cost: up to 59 minutes of staleness.

**Missing weather: rows kept, `wx_matched = false`.** Hit rate must be at least
99.5% (`WEATHER_HIT_RATE_MIN`) or the run halts. Whether to impute or exclude
the remainder is a Phase 5 decision.

**One `silver.flights_wx` table holds all statuses.** Cancelled flights keep
their weather so cancellations can be analysed against conditions;
`flight_status` separates them for OTP.

**Visibility is dropped.** Measured 100% null in the ERA5 archive. The spec
listed it as a useful delay predictor; it is not obtainable from this source.
Bronze keeps the raw JSON including the empty field.

**Hit rate was 100%** (582,425 / 582,425) after padding the date range. This
proves coverage only; correctness of the UTC hour rests on the hand-trace of
Phase 2 timestamps.

## Gold: reporting

**Two on-time rates, labelled.** BTS/DOT publish on-time as a share of all
scheduled flights (cancelled and diverted count as not on time). Ours on the
same basis: 72.82% vs published 72.8%. On-time as a share of *completed*
flights is 75.91%, a different metric that answers "when it flies, is it on
time?". Dashboards must state which one they show; the gap (3 pp) is the
cancellations and diversions.

**Reconciliation, January 2024:** scheduled flights 582,425 and cancellations
22,073 match the DOT Air Travel Consumer Report exactly; carrier-level rates
match to one decimal (AA 70.5, DL 77.8, AS cancel 11.9, UA cancel 6.9).
Source: https://www.bts.gov/newsroom/air-travel-consumer-report-january-2024-numbers

## Gold: features and model

**Feature table is an allow-list, plus a second assertion against a list of
known post-departure columns.** Why each excluded column would leak:

| Column(s) | Why unusable before departure |
|---|---|
| `DepTime`, `DepDelay`, `DepDel15` | Recorded at departure. Departure delay is the strongest predictor of arrival delay, and exactly what a pre-departure forecast cannot have. |
| `TaxiOut`, `WheelsOff` | Happen after pushback. |
| `WheelsOn`, `TaxiIn`, `ArrTime`, `ActualElapsedTime`, `AirTime` | Describe the flight itself or the arrival. |
| `ArrDelay`, `ArrDel15` | The target. |
| `CarrierDelay`, `WeatherDelay`, `NASDelay`, `SecurityDelay`, `LateAircraftDelay` | Assigned retrospectively and populated only when arrival delay is 15+ minutes, so their presence encodes the label. |
| `Cancelled`, `CancellationCode`, `Diverted` | Known only on the day; also define who is in the population. |
| `actual_dep_utc`, `actual_arr_utc` | Derived from the delays above. |

**Trailing history is keyed on actual arrival date and excludes the whole
current UTC day.** A flight scheduled yesterday evening may land after today's
flight departs, so its outcome is not yet known; excluding by scheduled date
would leak. The window is 30 days and a rate with fewer than 20 prior flights
is NULL (not 0), because "no history" is not "never late". An independent
plain-filter recompute of one flight's history is asserted equal to the table.

**Population = completed flights with origin weather.** Cancelled and diverted
flights have no arrival delay to predict. Rows with no weather are excluded
from the feature table only (count printed; zero in January 2024).

**Label = arrival delay of 15+ minutes** (BTS `ArrDel15`).

**Split: train to 20 Jan, validation 21-23 Jan (threshold choice only), gap
24 Jan, test 25-31 Jan.** Chronological, never random. One month is a real
limitation: the first days have no history, `month` is constant (dropped from
the model), and the test week is a single weather regime.

**Decision threshold is chosen on validation (max F1), then frozen for test.**
A missed delay and a false alarm have different costs; F1 is a neutral default,
not an operational recommendation.

## Model result, first run (January 2024)

Test = 25-31 Jan (+ the UTC-Feb-1 spill). AUC-PR: random 0.155, trailing-rate
baseline 0.210, logistic regression 0.201, gradient boosting 0.184. **The
models do not beat the baseline.** Using `dep_delay` alone (invalid, leaks)
scores 0.833, which is why the leakage rules exist.

Hypotheses, not yet tested: (1) regime shift - training contains the mid-Jan
storms, the test week's delay rate falls 22% -> 9%; (2) in the first training
days the "30-day" history is 1-5 days, so features mean something different in
train and test; (3) HistGradientBoosting's built-in early stopping uses a
random validation split, which is not chronological and likely overfits the
training period; (4) the strongest legitimate predictor, the inbound aircraft's
delay, is not in the feature set.

## Open
- Impute vs exclude for flights with no weather (Phase 5).
