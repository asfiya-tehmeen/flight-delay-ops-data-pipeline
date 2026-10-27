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

**DST:** `to_utc_timestamp` resolves the overlap hour to the earlier offset and
shifts times in the spring gap forward. January data never exercises this; it
must be re-checked before extending the date range across March or November.

## Open
- Weather join tolerance and missing-weather handling (Phase 3).
