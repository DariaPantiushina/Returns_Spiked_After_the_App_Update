DROP TABLE IF EXISTS marts.marts_kpi_daily;
CREATE TABLE marts.marts_kpi_daily AS
-- marts.marts_kpi_daily
WITH status_map AS (
    SELECT
		status_id,
		LOWER(status_name) AS st
    FROM dim.dim_order_status
),
paid_orders AS (
    SELECT
		f.user_id,
		f.order_id,
		f.order_date::date AS order_date, 
		f.amount
    FROM fact.fact_orders f
    JOIN status_map s ON s.status_id = f.status_id
    WHERE s.st IN ('paid', 'completed', 'delivered', 'success', 'approved')
),
daily_orders AS (
    SELECT
		order_date AS date, 
		COUNT(DISTINCT order_id) AS orders
    FROM paid_orders
    GROUP BY order_date
),
refund_orders AS (
    SELECT
		f.user_id,
		f.order_id, 
		f.order_date::date AS order_date, 
		f.amount
    FROM fact.fact_orders f
    JOIN status_map s ON s.status_id = f.status_id
    WHERE s.st IN ('refund', 'refunded', 'returned', 'chargeback')
),
daily_refunds AS (
    SELECT
		order_date AS date, 
		COUNT(DISTINCT order_id) AS refunds
    FROM refund_orders
    GROUP BY order_date
),
event_map AS (
    SELECT
		den.event_name_id, 
		LOWER(den.event_name) AS ev
    FROM dim.dim_event_name den
),
events_labeled AS (
    SELECT 
		fe.user_id, 
		fe.event_date::date AS date, 
		em.ev
    FROM fact.fact_events fe
    JOIN event_map em ON em.event_name_id = fe.event_name_id
    WHERE em.ev IN ('add_to_cart', 'checkout_start', 'payment_success', 'refund')
),
step_daily AS (
    SELECT
        date,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'add_to_cart')     AS users_cart,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'checkout_start')  AS users_checkout,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'payment_success') AS users_payment,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'refund')          AS users_refund
    FROM events_labeled
    GROUP BY date
),
funnel_daily AS (
    SELECT
        date,
        ROUND(100.0 * COUNT(DISTINCT user_id) FILTER (WHERE ev = 'checkout_start')
              / NULLIF(COUNT(DISTINCT user_id) FILTER (WHERE ev = 'add_to_cart'), 0), 2) AS conv_cart_to_checkout_pct,
        ROUND(100.0 * COUNT(DISTINCT user_id) FILTER (WHERE ev = 'payment_success')
              / NULLIF(COUNT(DISTINCT user_id) FILTER (WHERE ev = 'checkout_start'), 0), 2) AS conv_checkout_to_payment_pct,
        ROUND(100.0 * COUNT(DISTINCT user_id) FILTER (WHERE ev = 'refund')
              / NULLIF(COUNT(DISTINCT user_id) FILTER (WHERE ev = 'payment_success'), 0), 2) AS refund_after_payment_pct
    FROM events_labeled
    GROUP BY date
),
conv_daily AS (
    SELECT
        e.date,
        COUNT(*) FILTER (WHERE ev = 'payment_success')::float
          / NULLIF(COUNT(*) FILTER (WHERE ev = 'add_to_cart'), 0) AS conv_to_payment
    FROM events_labeled e
    GROUP BY e.date
),
first_purchase AS (
    SELECT 
		user_id, 
		MIN(order_date) AS first_paid_date
    FROM paid_orders
    GROUP BY user_id
),
retained_d7 AS (
    SELECT 
		p2.order_date AS date, 
		COUNT(DISTINCT p2.user_id) AS retained_users_d7
    FROM paid_orders p2
    JOIN first_purchase fp ON fp.user_id = p2.user_id
                          AND p2.order_date = fp.first_paid_date + INTERVAL '7 day'
    GROUP BY p2.order_date
),
cohort_d7 AS (
    SELECT 
		fp.first_paid_date + INTERVAL '7 day' AS date,
        COUNT(DISTINCT fp.user_id) AS cohort_users_d7
    FROM first_purchase fp
    GROUP BY fp.first_paid_date + INTERVAL '7 day'
)
SELECT
    d.date,
    COALESCE(o.orders, 0) AS orders,
    COALESCE(r.refunds, 0) AS refunds,
    CASE WHEN COALESCE(o.orders, 0) = 0
         THEN NULL
         ELSE COALESCE(r.refunds, 0)::float / o.orders
    END AS return_rate,
    conv.conv_to_payment,
    f.conv_cart_to_checkout_pct,
    f.conv_checkout_to_payment_pct,
    f.refund_after_payment_pct,
    s.users_cart,
    s.users_checkout,
    s.users_payment,
    s.users_refund,
    CASE WHEN COALESCE(c.cohort_users_d7, 0) = 0
         THEN NULL
         ELSE COALESCE(ret.retained_users_d7, 0)::float / c.cohort_users_d7
    END AS d7_retention
FROM (
    SELECT date
    FROM (
        SELECT order_date::date AS date FROM paid_orders
        UNION
        SELECT order_date::date AS date FROM refund_orders
        UNION
        SELECT event_date::date AS date FROM fact.fact_events
    ) x
    GROUP BY date
) d
LEFT JOIN daily_orders o ON o.date = d.date
LEFT JOIN daily_refunds r ON r.date = d.date
LEFT JOIN conv_daily conv ON conv.date = d.date
LEFT JOIN funnel_daily f ON f.date = d.date
LEFT JOIN step_daily s   ON s.date = d.date
LEFT JOIN retained_d7 ret ON ret.date = d.date
LEFT JOIN cohort_d7 c ON c.date = d.date
ORDER BY d.date;

--#######################################################################################--

-- Optimized Query

-- Materialized reference tables (small but reused often) 
CREATE MATERIALIZED VIEW dim.status_map_mv AS
SELECT status_id, LOWER(status_name) AS st
FROM dim.dim_order_status;

CREATE MATERIALIZED VIEW dim.event_map_mv AS
SELECT event_name_id, LOWER(event_name) AS ev
FROM dim.dim_event_name;

-- Indexes for fact_orders (filtering and grouping on status_id, order_date, user_id, order_id)
CREATE INDEX idx_fact_orders_status_date ON fact.fact_orders (status_id, order_date);
CREATE INDEX idx_fact_orders_user_order  ON fact.fact_orders (user_id, order_id);

-- Indexes for fact_events (filtering and grouping on event_name_id, event_date, user_id)
CREATE INDEX idx_fact_events_event_date ON fact.fact_events (event_name_id, event_date);
CREATE INDEX idx_fact_events_user_date  ON fact.fact_events (user_id, event_date);

DROP TABLE IF EXISTS marts.marts_kpi_daily;
CREATE TABLE marts.marts_kpi_daily AS
-- Using materialized views for status_map and event_map
WITH paid_orders AS (
    SELECT
        f.user_id,
        f.order_id,
        f.order_date::date AS order_date, 
        f.amount
    FROM fact.fact_orders f
    JOIN dim.status_map_mv s ON s.status_id = f.status_id
    WHERE s.st IN ('paid', 'completed', 'delivered', 'success', 'approved')
),
daily_orders AS (
    SELECT
        order_date AS date, 
        COUNT(DISTINCT order_id) AS orders
    FROM paid_orders
    GROUP BY order_date
),
refund_orders AS (
    SELECT
        f.user_id,
        f.order_id, 
        f.order_date::date AS order_date, 
        f.amount
    FROM fact.fact_orders f
    JOIN dim.status_map_mv s ON s.status_id = f.status_id
    WHERE s.st IN ('refund', 'refunded', 'returned', 'chargeback')
),
daily_refunds AS (
    SELECT
        order_date AS date, 
        COUNT(DISTINCT order_id) AS refunds
    FROM refund_orders
    GROUP BY order_date
),
events_labeled AS (
    SELECT 
        fe.user_id, 
        fe.event_date::date AS date, 
        em.ev
    FROM fact.fact_events fe
    JOIN dim.event_map_mv em ON em.event_name_id = fe.event_name_id
    WHERE em.ev IN ('add_to_cart', 'checkout_start', 'payment_success', 'refund')
),
step_daily AS (
    SELECT
        date,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'add_to_cart')     AS users_cart,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'checkout_start')  AS users_checkout,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'payment_success') AS users_payment,
        COUNT(DISTINCT user_id) FILTER (WHERE ev = 'refund')          AS users_refund
    FROM events_labeled
    GROUP BY date
),
funnel_daily AS (
    -- Daily conversion rates between steps
    SELECT
        date,
        ROUND(100.0 * COUNT(DISTINCT user_id) FILTER (WHERE ev = 'checkout_start')
              / NULLIF(COUNT(DISTINCT user_id) FILTER (WHERE ev = 'add_to_cart'), 0), 2) AS conv_cart_to_checkout_pct,
        ROUND(100.0 * COUNT(DISTINCT user_id) FILTER (WHERE ev = 'payment_success')
              / NULLIF(COUNT(DISTINCT user_id) FILTER (WHERE ev = 'checkout_start'), 0), 2) AS conv_checkout_to_payment_pct,
        ROUND(100.0 * COUNT(DISTINCT user_id) FILTER (WHERE ev = 'refund')
              / NULLIF(COUNT(DISTINCT user_id) FILTER (WHERE ev = 'payment_success'), 0), 2) AS refund_after_payment_pct
    FROM events_labeled
    GROUP BY date
),
conv_daily AS (
    -- Overall daily conversion (add_to_cart → payment_success)
    SELECT
        e.date,
        COUNT(*) FILTER (WHERE ev = 'payment_success')::float
          / NULLIF(COUNT(*) FILTER (WHERE ev = 'add_to_cart'), 0) AS conv_to_payment
    FROM events_labeled e
    GROUP BY e.date
),
first_purchase AS (
    SELECT 
        user_id, 
        MIN(order_date) AS first_paid_date
    FROM paid_orders
    GROUP BY user_id
),
retained_d7 AS (
    SELECT 
        p2.order_date AS date, 
        COUNT(DISTINCT p2.user_id) AS retained_users_d7
    FROM paid_orders p2
    JOIN first_purchase fp ON fp.user_id = p2.user_id
                          AND p2.order_date = fp.first_paid_date + INTERVAL '7 day'
    GROUP BY p2.order_date
),
cohort_d7 AS (
    SELECT 
        fp.first_paid_date + INTERVAL '7 day' AS date,
        COUNT(DISTINCT fp.user_id) AS cohort_users_d7
    FROM first_purchase fp
    GROUP BY fp.first_paid_date + INTERVAL '7 day'
)
SELECT
    d.date,
    COALESCE(o.orders, 0) AS orders,
    COALESCE(r.refunds, 0) AS refunds,
    CASE WHEN COALESCE(o.orders, 0) = 0
         THEN NULL
         ELSE COALESCE(r.refunds, 0)::float / o.orders
    END AS return_rate,
    conv.conv_to_payment,
    f.conv_cart_to_checkout_pct,
    f.conv_checkout_to_payment_pct,
    f.refund_after_payment_pct,
    s.users_cart,
    s.users_checkout,
    s.users_payment,
    s.users_refund,
    CASE WHEN COALESCE(c.cohort_users_d7, 0) = 0
         THEN NULL
         ELSE COALESCE(ret.retained_users_d7, 0)::float / c.cohort_users_d7
    END AS d7_retention
FROM (
    SELECT DISTINCT date
    FROM (
        SELECT order_date::date AS date FROM paid_orders
        UNION ALL
        SELECT order_date::date FROM refund_orders
        UNION ALL
        SELECT event_date::date FROM fact.fact_events
    ) x
) d
LEFT JOIN daily_orders o ON o.date = d.date
LEFT JOIN daily_refunds r ON r.date = d.date
LEFT JOIN conv_daily conv ON conv.date = d.date
LEFT JOIN funnel_daily f ON f.date = d.date
LEFT JOIN step_daily s   ON s.date = d.date
LEFT JOIN retained_d7 ret ON ret.date = d.date
LEFT JOIN cohort_d7 c ON c.date = d.date
ORDER BY d.date;

--#######################################################################################--
--#######################################################################################--

DROP TABLE IF EXISTS marts.marts_segments;
CREATE TABLE marts.marts_segments AS
WITH status_map AS (
	SELECT
		status_id,
		LOWER(status_name) AS st
	FROM dim.dim_order_status
),
paid_orders AS (
	SELECT
		fo.user_id,
        fo.order_id,
        fo.amount,
        fo.order_date::date AS order_date,
        du.device,
        du.region,
        du.channel
  FROM fact.fact_orders fo
  JOIN status_map sm ON sm.status_id = fo.status_id
  JOIN dim.dim_user du ON du.user_id = fo.user_id
  WHERE sm.st IN ('paid', 'completed', 'delivered', 'success', 'approved')
),
refund_orders AS (
	SELECT
		fo.user_id,
        fo.order_id,
        fo.amount,
        fo.order_date::date AS order_date,
        du.device,
        du.region,
        du.channel
	FROM fact.fact_orders fo
	JOIN status_map sm ON sm.status_id = fo.status_id
	JOIN dim.dim_user du ON du.user_id = fo.user_id
  	WHERE sm.st IN ('refund', 'refunded', 'returned', 'chargeback')
),
agg_orders AS (
	SELECT
    	po.order_date AS date,
    	po.device,
    	po.region,
    	po.channel,
    	COUNT(*) AS orders
  	FROM paid_orders po
  	GROUP BY po.order_date, po.device, po.region, po.channel
),
agg_refunds AS (
  	SELECT
    	ro.order_date AS date,
    	ro.device,
    	ro.region,
    	ro.channel,
    	COUNT(*) AS refunds
  	FROM refund_orders ro
  	GROUP BY ro.order_date, ro.device, ro.region, ro.channel
),
grid AS (
	SELECT
    	d.date,
    	s.device,
    	s.region,
    	s.channel
  	FROM (
    	SELECT
			date 
		FROM (
      		SELECT 
				DISTINCT order_date::date AS date 
			FROM fact.fact_orders
    	) dd
  	) d
  	CROSS JOIN (
    		SELECT
				DISTINCT du.device, du.region, du.channel
    		FROM dim.dim_user du
  	) s
)
SELECT
	g.date,
  	g.device,
  	g.region,
  	g.channel,
  	COALESCE(o.orders, 0) AS orders,
  	COALESCE(r.refunds, 0) AS refunds,
  	CASE
    	WHEN COALESCE(o.orders, 0) = 0
		THEN NULL ELSE COALESCE(r.refunds, 0)::decimal / o.orders
  	END AS return_rate
FROM grid g
LEFT JOIN agg_orders o ON o.date = g.date AND o.device = g.device AND o.region = g.region AND o.channel = g.channel
LEFT JOIN agg_refunds r ON r.date = g.date AND r.device = g.device AND r.region = g.region AND r.channel = g.channel
WHERE o.orders IS NOT NULL OR r.refunds IS NOT NULL
ORDER BY g.date, g.device, g.region, g.channel;