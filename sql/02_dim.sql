-- DIM: dim_user
DROP TABLE IF EXISTS dim.dim_user;
CREATE TABLE dim.dim_user AS
SELECT 
	DISTINCT user_id, region, device, channel
FROM stg.stg_users_clean;

-- DIM: dim_date
DROP TABLE IF EXISTS dim.dim_date;
CREATE TABLE dim.dim_date AS
WITH bounds AS (
	SELECT 
    	MIN(d)::date AS min_date,
    	MAX(d)::date AS max_date
  	FROM (
    		SELECT
				reg_date::date AS d
			FROM stg.stg_users_clean
    		UNION ALL
    		SELECT
				event_date::date 
			FROM stg.stg_events_clean
  ) t
),
series AS (
	SELECT
		generate_series(b.min_date, b.max_date, interval '1 day')::date AS date_key
  	FROM bounds b
)
SELECT
  	s.date_key,
  	EXTRACT(ISODOW FROM s.date_key) AS iso_dow,
  	EXTRACT(DAY FROM s.date_key) AS day_of_month,
  	EXTRACT(WEEK FROM s.date_key) AS week_of_year,
  	EXTRACT(MONTH FROM s.date_key) AS month,
  	TO_CHAR(s.date_key, 'Month') AS month_name,
  	EXTRACT(QUARTER FROM s.date_key) AS quarter,
  	EXTRACT(YEAR FROM s.date_key) AS year,
  	DATE_TRUNC('week', s.date_key)::date AS week_start,
  	DATE_TRUNC('month', s.date_key)::date AS month_start,
  	DATE_TRUNC('quarter', s.date_key)::date AS quarter_start,
  	DATE_TRUNC('year', s.date_key)::date AS year_start,
  	EXTRACT(ISODOW FROM s.date_key) IN (6,7) AS is_weekend
FROM series s;

---
ALTER TABLE dim_date ADD PRIMARY KEY (date_key);
CREATE INDEX IF NOT EXISTS idx_users_reg_date ON stg.stg_users_clean (reg_date);
CREATE INDEX IF NOT EXISTS idx_events_event_date ON stg.stg_events_clean (event_date);
---

-- DIM: order_status
DROP TABLE IF EXISTS dim.dim_order_status;
CREATE TABLE dim.dim_order_status (
  status_id serial PRIMARY KEY,
  status_name text UNIQUE
);
INSERT INTO dim.dim_order_status (status_name)
SELECT DISTINCT status
FROM stg.stg_orders_clean;

-- DIM: event_name
DROP TABLE IF EXISTS dim.dim_event_name;
CREATE TABLE dim.dim_event_name (
    event_name_id SERIAL PRIMARY KEY, 
    event_name TEXT UNIQUE
);
INSERT INTO dim.dim_event_name (event_name)
SELECT DISTINCT event_name
FROM stg.stg_events_clean;