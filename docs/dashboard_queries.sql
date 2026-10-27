-- Databricks SQL dashboard queries for the Flight Operations Pipeline.
-- Run each query in the SQL Editor, then "Add to dashboard" (or build a
-- Dashboard -> Add data -> paste the query as a dataset).
--
-- METRIC DEFINITIONS (shown on the dashboard so nobody mixes them up):
--   on_time_pct_scheduled : on-time arrivals / ALL scheduled flights.
--                           Cancelled and diverted count as not on time.
--                           This is the DOT / BTS published basis.
--   on_time_pct_completed : on-time arrivals / flights that completed.
--                           "When it flies, is it on time?"
-- On time = arrival delay < 15 minutes.
-- Weather (c) Open-Meteo, CC BY 4.0. Flights: US DOT / BTS.

USE CATALOG flight_ops;

-- ============================================================
-- 1. Headline KPIs  (counter tiles)
-- ============================================================
SELECT
  COUNT(*)                                                          AS flights_scheduled,
  ROUND(100 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                    AS on_time_pct_scheduled,
  ROUND(100 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END)
        / SUM(CASE WHEN flight_status = 'completed' THEN 1 ELSE 0 END), 2)
                                                                    AS on_time_pct_completed,
  ROUND(100 * SUM(CASE WHEN flight_status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                    AS cancelled_pct,
  ROUND(100 * SUM(CASE WHEN flight_status = 'diverted'  THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                    AS diverted_pct
FROM gold.fct_flight_performance;

-- ============================================================
-- 2. Daily trend  (line chart: x = flight_date)
--    Shows the mid-January storm period.
-- ============================================================
SELECT
  flight_date,
  COUNT(*)                                                                   AS flights,
  ROUND(100 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2)     AS on_time_pct_scheduled,
  ROUND(100 * SUM(CASE WHEN flight_status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                             AS cancelled_pct,
  ROUND(AVG(CASE WHEN flight_status = 'completed' THEN arr_delay_min END), 1) AS avg_arr_delay_min
FROM gold.fct_flight_performance
GROUP BY flight_date
ORDER BY flight_date;

-- ============================================================
-- 3. Carrier league table  (bar chart or table)
--    Marketing carrier = the brand on the ticket (includes regional partners).
-- ============================================================
SELECT
  marketing_carrier,
  flights_scheduled,
  ROUND(100 * flights_on_time / flights_scheduled, 2)  AS on_time_pct_scheduled,
  ROUND(100 * on_time_rate, 2)                         AS on_time_pct_completed,
  ROUND(100 * cancel_rate, 2)                          AS cancelled_pct,
  median_arr_delay_min,
  avg_arr_delay_min
FROM gold.agg_carrier_otp
ORDER BY flights_scheduled DESC;

-- ============================================================
-- 4. Worst routes  (table; minimum volume so tiny routes don't dominate)
-- ============================================================
SELECT
  origin, dest,
  flights_scheduled,
  ROUND(100 * flights_on_time / flights_scheduled, 2)  AS on_time_pct_scheduled,
  ROUND(100 * cancel_rate, 2)                          AS cancelled_pct,
  median_arr_delay_min
FROM gold.agg_route_otp
WHERE flights_scheduled >= 200
ORDER BY on_time_pct_scheduled ASC
LIMIT 15;

-- ============================================================
-- 5. Busiest airports - departure on-time  (table / bar)
--    Departures and arrivals are different populations, so kept separate.
-- ============================================================
SELECT
  airport,
  flights_scheduled,
  ROUND(100 * on_time_rate, 2)  AS departure_on_time_pct,
  ROUND(100 * flights_cancelled / flights_scheduled, 2) AS cancelled_pct,
  median_delay_min
FROM gold.agg_airport_otp
WHERE role = 'departure'
ORDER BY flights_scheduled DESC
LIMIT 20;

-- ============================================================
-- 6. Time of day  (bar chart)
-- ============================================================
SELECT
  dep_time_block,
  flights_scheduled,
  ROUND(100 * flights_on_time / flights_scheduled, 2)  AS on_time_pct_scheduled,
  ROUND(100 * cancel_rate, 2)                          AS cancelled_pct
FROM gold.agg_time_block_otp
ORDER BY dep_time_block;

-- ============================================================
-- 7. Day of week  (bar chart; 1 = Monday ... 7 = Sunday)
-- ============================================================
SELECT
  day_of_week,
  COUNT(*)                                                                   AS flights,
  ROUND(100 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2)     AS on_time_pct_scheduled
FROM gold.fct_flight_performance
GROUP BY day_of_week
ORDER BY day_of_week;

-- ============================================================
-- 8. Weather impact: snowfall at origin at scheduled departure hour
--    (shows what the weather join buys; snowfall is cm per hour)
-- ============================================================
SELECT
  CASE
    WHEN wx_snowfall = 0    THEN '1 none'
    WHEN wx_snowfall <= 0.5 THEN '2 light (<=0.5 cm/h)'
    WHEN wx_snowfall <= 2   THEN '3 moderate (<=2 cm/h)'
    ELSE                         '4 heavy (>2 cm/h)'
  END                                                                        AS snowfall_bucket,
  COUNT(*)                                                                   AS flights,
  ROUND(100 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2)     AS on_time_pct_scheduled,
  ROUND(100 * SUM(CASE WHEN flight_status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                             AS cancelled_pct
FROM gold.fct_flight_performance
WHERE wx_matched
GROUP BY 1
ORDER BY 1;

-- ============================================================
-- 9. Weather impact: wind gusts at origin (km/h)
-- ============================================================
SELECT
  CASE
    WHEN wx_wind_gusts_10m < 30 THEN '1 calm (<30 km/h)'
    WHEN wx_wind_gusts_10m < 50 THEN '2 breezy (30-50)'
    WHEN wx_wind_gusts_10m < 70 THEN '3 strong (50-70)'
    ELSE                             '4 severe (70+)'
  END                                                                        AS gust_bucket,
  COUNT(*)                                                                   AS flights,
  ROUND(100 * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) / COUNT(*), 2)     AS on_time_pct_scheduled
FROM gold.fct_flight_performance
WHERE wx_matched
GROUP BY 1
ORDER BY 1;

-- ============================================================
-- 10. Pipeline health  (small table: proves the quality story)
-- ============================================================
SELECT 'flights in bronze'      AS metric, COUNT(*) AS value FROM bronze.flights_raw
UNION ALL SELECT 'flights in silver (all statuses)', COUNT(*) FROM silver.flights_wx
UNION ALL SELECT 'flights with origin weather',      COUNT(*) FROM silver.flights_wx WHERE wx_matched
UNION ALL SELECT 'flights in model feature table',   COUNT(*) FROM gold.feat_delay_model;
