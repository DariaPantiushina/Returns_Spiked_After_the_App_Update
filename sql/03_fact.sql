-- FACT: fact_orders
DROP TABLE IF EXISTS fact.fact_orders;
CREATE TABLE fact.fact_orders AS
SELECT
  o.order_id,
  o.user_id,
  o.order_date::timestamp,
  o.amount,
  ds.status_id
FROM stg.stg_orders_clean o
JOIN dim.dim_order_status ds ON ds.status_name = o.status;

-- FACT: fact_events
DROP TABLE IF EXISTS fact.fact_events;
CREATE TABLE fact.fact_events AS
SELECT
  e.user_id,
  e.event_date::timestamp,
  dn.event_name_id
FROM stg.stg_events_clean e
JOIN dim.dim_event_name dn ON dn.event_name = e.event_name;