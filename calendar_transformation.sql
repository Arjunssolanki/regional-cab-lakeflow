-- ====================================================================
-- SILVER LAYER: COMPREHENSIVE CALENDAR DIMENSION (Indian Holidays)
-- ====================================================================
CREATE OR REFRESH LIVE TABLE transportation.silver.calendar
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'silver',
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT "Calendar dimension with comprehensive date attributes and Indian holidays"
AS 
WITH date_sequence AS (
  SELECT explode(sequence(
    CAST('${start_date}' AS DATE),
    CAST('${end_date}' AS DATE),
    INTERVAL 1 DAY
  )) AS date
),
calendar_attributes AS (
  SELECT
    date,
    CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
    year(date) AS year,
    month(date) AS month,
    dayofmonth(date) AS day_of_month,
    date_format(date, 'EEEE') AS day_of_week,
    date_format(date, 'EEE') AS day_of_week_abbr,
    dayofweek(date) AS day_of_week_num,
    date_format(date, 'MMMM') AS month_name,
    concat(date_format(date, 'MMMM'), ' ', year(date)) AS month_year,
    quarter(date) AS quarter,
    concat('Q', quarter(date), ' ', year(date)) AS quarter_year,
    weekofyear(date) AS week_of_year,
    dayofyear(date) AS day_of_year
  FROM date_sequence
),
holiday_logic AS (
  SELECT
    *,
    CASE 
      WHEN day_of_week_num IN (1, 7) THEN true 
      ELSE false 
    END AS is_weekend,
    CASE 
      WHEN day_of_week_num IN (1, 7) THEN false 
      ELSE true 
    END AS is_weekday,
    CASE
      WHEN month = 1 AND day_of_month = 26 THEN 'Republic Day'
      WHEN month = 8 AND day_of_month = 15 THEN 'Independence Day'
      WHEN month = 10 AND day_of_month = 2 THEN 'Gandhi Jayanti'
      ELSE NULL
    END AS holiday_name
  FROM calendar_attributes
)
SELECT
  date,
  date_key,
  year,
  month,
  day_of_month,
  day_of_week,
  day_of_week_abbr,
  month_name,
  month_year,
  quarter,
  quarter_year,
  week_of_year,
  day_of_year,
  is_weekday,
  is_weekend,
  CASE 
    WHEN holiday_name IS NOT NULL THEN true 
    ELSE false 
  END AS is_holiday,
  holiday_name,
  current_timestamp() AS silver_processed_timestamp
FROM holiday_logic;
