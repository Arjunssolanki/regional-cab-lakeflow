-- ====================================================================
-- 1. MASTER GOLD LAYER: COMBINED TRIP FACT VIEW (Routed to Gold Folder)
-- ====================================================================
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips -- 👈 Explicitly routes to Gold folder
COMMENT "Master Gold dataset combining trip analytics with temporal calendars and city context"
AS (
  SELECT 
    t.id,
    t.business_date,
    t.city_id,
    c.city_name,
    t.passenger_category,
    t.distance_kms,
    t.sales_amt,
    t.passenger_rating,
    t.driver_rating,
    ca.month,
    ca.day_of_month,
    ca.day_of_week,
    ca.month_name,
    ca.month_year,
    ca.quarter,
    ca.quarter_year,
    ca.week_of_year,
    ca.is_weekday,
    ca.is_weekend,
    ca.is_holiday AS national_holiday
  FROM live.silver_trips t              -- Reads dynamically from your live Silver table
  INNER JOIN live.silver_city c        ON t.city_id = c.city_id
  INNER JOIN live.silver_calendar ca  ON t.business_date = ca.date
);

-- ====================================================================
-- 2. REGIONAL GOLD LAYER: FILTERED REGIONAL SLICES (Routed to Gold Folder)
-- ====================================================================
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_chandigarh    AS (SELECT * FROM live.fact_trips WHERE city_id = 'CH01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_coimbatore    AS (SELECT * FROM live.fact_trips WHERE city_id = 'TN01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_indore        AS (SELECT * FROM live.fact_trips WHERE city_id = 'MP01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_jaipur        AS (SELECT * FROM live.fact_trips WHERE city_id = 'RJ01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_kochi         AS (SELECT * FROM live.fact_trips WHERE city_id = 'KL01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_lucknow       AS (SELECT * FROM live.fact_trips WHERE city_id = 'UP01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_mysore        AS (SELECT * FROM live.fact_trips WHERE city_id = 'KA01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_surat         AS (SELECT * FROM live.fact_trips WHERE city_id = 'GJ01');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_vadodara      AS (SELECT * FROM live.fact_trips WHERE city_id = 'GJ02');
CREATE OR REFRESH LIVE VIEW transportation.gold.fact_trips_visakhapatnam AS (SELECT * FROM live.fact_trips WHERE city_id = 'AP01');
