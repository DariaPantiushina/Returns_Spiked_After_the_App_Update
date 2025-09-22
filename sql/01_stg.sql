DROP TABLE IF EXISTS stg.stg_users_raw;
CREATE TABLE stg.stg_users_raw (
  user_id         int PRIMARY KEY,
  reg_date        date,
  device          text,     
  region          text,     
  channel         text      
);

DROP TABLE IF EXISTS stg.stg_users_clean;
CREATE TABLE stg.stg_users_clean AS
WITH dedup AS (
    SELECT
        user_id,
        MIN(reg_date::date) AS reg_date,
        CASE 
            WHEN LOWER(device) = 'ios' THEN 'iOS'
            WHEN LOWER(device) = 'android' THEN 'Android'
            ELSE device
        END AS device,
        UPPER(region) AS region,
        CASE 
            WHEN LOWER(channel) = 'paid' THEN 'Paid'
            WHEN LOWER(channel) = 'organic' THEN 'Organic'
            ELSE channel
        END AS channel,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY reg_date) AS rn
    FROM stg.stg_users_raw
    GROUP BY user_id, device, region, channel
)
SELECT user_id, reg_date, device, region, channel
FROM dedup
WHERE rn = 1;

--####################################################################################--

DROP TABLE IF EXISTS stg.stg_orders_raw;
CREATE TABLE stg.stg_orders_raw (
  order_id      text PRIMARY KEY,
  user_id       int,
  order_date    timestamp,
  amount        numeric(8,2),
  status        text        
);

DROP TABLE IF EXISTS stg.stg_orders_clean;
CREATE TABLE stg.stg_orders_clean AS
WITH dedup AS (
    SELECT
        order_id,
        user_id,
        order_date::timestamp AS order_date,
        amount::numeric(8,2) AS amount,
        LOWER(status) AS status,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date) AS rn
    FROM stg.stg_orders_raw
    WHERE amount >= 0
)
SELECT order_id, user_id, order_date, amount, status
FROM dedup
WHERE rn = 1;

--####################################################################################--

DROP TABLE IF EXISTS stg.stg_events_raw;
CREATE TABLE stg.stg_events_raw (
  event_id      text PRIMARY KEY,
  user_id       int,
  event_date    timestamp,
  event_name    text        
);

DROP TABLE IF EXISTS stg.stg_events_clean;
CREATE TABLE stg.stg_events_clean AS
WITH dedup AS (
    SELECT
        event_id,
        user_id,
        event_date::timestamp AS event_date,
        LOWER(event_name) AS event_name,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_date) AS rn
    FROM stg.stg_events_raw
)
SELECT event_id, user_id, event_date, event_name
FROM dedup
WHERE rn = 1;