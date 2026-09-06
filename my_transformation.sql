-- ====================================================================
-- 1. BRONZE LAYER: INGEST RAW CITY DATA FROM S3
-- ====================================================================
CREATE OR REFRESH STREAMING TABLE bronze_dim_city_raw -- Renamed for uniqueness
TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'bronze',
  'source_format' = 'csv',
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT "City Raw Data Processing"
AS SELECT 
  *,
  _metadata.file_path AS file_name,
  current_timestamp() AS ingest_datetime
FROM cloud_files(
  "s3://goodcabs-as1/data-store/city/",
  "csv",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaEvolutionMode", "rescue"
  )
);

-- ====================================================================
-- 2. BRONZE LAYER: INGEST RAW TRIPS DATA FROM S3
-- ====================================================================
CREATE OR REFRESH STREAMING TABLE bronze_trips -- Renamed to prevent collision!
TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'bronze',
  'source_format' = 'csv',
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT "Streaming ingestion of raw orders data with Auto Loader"
AS SELECT
  trip_id,
  date,
  city_id,
  passenger_type,
  `distance_travelled(km)` AS distance_travelled_km,
  fare_amount,
  passenger_rating,
  driver_rating,
  _metadata.file_path AS file_name,
  current_timestamp() AS ingest_datetime
FROM cloud_files(
  "s3://goodcabs-as1/data-store/trips/",
  "csv",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaEvolutionMode", "rescue",
    "cloudFiles.maxFilesPerTrigger", "100"
  )
);
