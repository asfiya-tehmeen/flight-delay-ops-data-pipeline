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

## Open
- Impute vs exclude for flights with no weather (Phase 5).
