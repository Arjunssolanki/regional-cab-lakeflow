-- ====================================================================
-- 1. SILVER LAYER: CITY DIMENSION
-- ====================================================================
CREATE OR REFRESH STREAMING TABLE transportation.silver.city
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT "Cleaned and standardized products dimension with business transformations"
AS SELECT 
  city_id AS city_id,
  city_name AS city_name,
  ingest_datetime AS bronze_ingest_timestamp,
  current_timestamp() AS silver_processed_timestamp
FROM STREAM(live.bronze_dim_city_raw);


-- ====================================================================
-- 2. SILVER LAYER: TRIPS STAGING (Kept as pipeline view)
-- ====================================================================
CREATE OR REFRESH STREAMING TABLE trips_silver_staging
COMMENT "Transformed trips data ready for CDC upsert"
AS SELECT
  trip_id AS id,
  CAST(date AS DATE) AS business_date,
  city_id AS city_id,
  LOWER(passenger_type) AS passenger_category,
  distance_travelled_km AS distance_kms,
  fare_amount AS sales_amt,
  passenger_rating AS passenger_rating,
  driver_rating AS driver_rating,
  ingest_datetime AS bronze_ingest_timestamp,
  current_timestamp() AS silver_processed_timestamp
FROM STREAM(live.bronze_trips);


-- ====================================================================
-- 3. SILVER LAYER: FINAL TRIPS TARGET (SCD Type 1)
-- ====================================================================
CREATE OR REFRESH STREAMING TABLE transportation.silver.trips
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT "Cleaned and validated orders with CDC upsert capability";

APPLY CHANGES INTO transportation.silver.trips
FROM STREAM(live.trips_silver_staging)
KEYS (id)
SEQUENCE BY silver_processed_timestamp
STORED AS SCD TYPE 1;
