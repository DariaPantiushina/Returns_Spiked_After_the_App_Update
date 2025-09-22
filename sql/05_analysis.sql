-- Return Rate (RR) before vs after 2025-06-15

WITH status_map AS (
	SELECT
		status_id, 
		LOWER(status_name) AS st
  	FROM dim.dim_order_status
),
fact_orders_filtered AS (
	SELECT
		*
	FROM fact.fact_orders fo
	WHERE fo.order_date BETWEEN DATE '2025-06-01' AND DATE '2025-06-30'
),
paid_orders AS (
  	SELECT
		fo.order_id,
		fo.user_id,
		fo.order_date::date AS date_key
  	FROM fact_orders_filtered fo
  	JOIN status_map sm ON sm.status_id = fo.status_id
  	WHERE sm.st IN ('paid', 'completed', 'delivered', 'success', 'approved')
),
refund_orders AS (
  	SELECT
		fo.order_id,
		fo.user_id,
		fo.order_date::date AS date_key
  	FROM fact_orders_filtered fo
  	JOIN status_map sm ON sm.status_id = fo.status_id
  	WHERE sm.st IN ('refund', 'refunded', 'returned', 'chargeback')
),
periods AS (
  	SELECT
    	CASE
			WHEN date_key < DATE '2025-06-15' 
			THEN 'before' ELSE 'after' 
		END AS period,
    	COUNT(*) FILTER (WHERE src = 'orders') AS orders,
    	COUNT(*) FILTER (WHERE src = 'refunds') AS refunds
  	FROM (
    	SELECT
			date_key, 
			'orders' AS src FROM paid_orders
    		UNION ALL
    	SELECT
			date_key,
			'refunds' AS src FROM refund_orders
  		) u
  	GROUP BY
		CASE 
			WHEN date_key < DATE '2025-06-15' 
			THEN 'before' ELSE 'after' 
		END
),
rr AS (
  	SELECT
    	period,
    	orders,
    	refunds,
    	CASE 
			WHEN NULLIF(orders, 0) IS NULL 
			THEN NULL ELSE ROUND(100.0 * (refunds::decimal / orders), 2) END AS return_rate
  	FROM periods
)
SELECT
  	MAX(CASE WHEN period = 'before' THEN orders END) AS orders_before,
  	MAX(CASE WHEN period = 'before' THEN refunds END) AS refunds_before,
  	MAX(CASE WHEN period = 'before' THEN return_rate END) AS return_rate_before,
  	MAX(CASE WHEN period = 'after' THEN orders END) AS orders_after,
  	MAX(CASE WHEN period = 'after' THEN refunds END) AS refunds_after,
  	MAX(CASE WHEN period = 'after' THEN return_rate END) AS return_rate_after,
  	CASE
    	WHEN MAX(CASE WHEN period = 'before' THEN return_rate END) IS NULL
         	OR MAX(CASE WHEN period = 'before' THEN return_rate END) = 0
    	THEN NULL
    	ELSE ROUND((MAX(CASE WHEN period = 'after' THEN return_rate END)
         	/ MAX(CASE WHEN period = 'before' THEN return_rate END) - 1.0) * 100.0, 2)
  	END AS return_rate_pct_change_after_vs_before
FROM rr;

-- Return Rate (RR). A/B comparison [before and after UI release] segments by: 
---- Android vs iOS (device type); 
---- EU vs US (region);
---- Paid vs Organic (user's channel) 

WITH params AS (
    SELECT
        DATE '2025-06-15' AS redesign_date
),
fo AS (
    SELECT
        f.order_id,
        f.user_id,
        f.order_date::date AS order_date,
        CASE
            WHEN LOWER(dos.status_name) IN ('paid', 'completed', 'delivered', 'success', 'approved') THEN 'paid'
            WHEN LOWER(dos.status_name) IN ('refund', 'refunded', 'returned', 'chargeback') THEN 'refund'
            ELSE 'other'
        END AS status_group
    FROM fact.fact_orders f
    JOIN dim.dim_order_status dos ON dos.status_id = f.status_id
),
labeled AS (
    SELECT
        CASE
            WHEN fo.order_date < p.redesign_date THEN 'before'
            ELSE 'after'
        END AS period,
        du.device,
        du.region,
        du.channel,
        (CASE WHEN fo.status_group = 'paid' THEN 1 ELSE 0 END) AS is_order,
        (CASE WHEN fo.status_group = 'refund' THEN 1 ELSE 0 END) AS is_refund
    FROM fo
    JOIN dim.dim_user du ON du.user_id = fo.user_id
    CROSS JOIN params p
    WHERE fo.order_date BETWEEN (p.redesign_date - INTERVAL '30 day') AND (p.redesign_date + INTERVAL '30 day')
),
agg AS (
    SELECT
        device,
        region,
        channel,
        period,
        SUM(is_order) AS orders,
        SUM(is_refund) AS refunds
    FROM labeled
    GROUP BY device, region, channel, period
),
pivot AS (
    SELECT
        device,
        region,
        channel,
        SUM(orders)  FILTER (WHERE period = 'before') AS orders_before,
        SUM(refunds) FILTER (WHERE period = 'before') AS refunds_before,
        SUM(orders)  FILTER (WHERE period = 'after')  AS orders_after,
        SUM(refunds) FILTER (WHERE period = 'after')  AS refunds_after
    FROM agg
    GROUP BY device, region, channel
),
calc AS (
    SELECT
        device,
        region,
        channel,
        orders_before,
        refunds_before,
        orders_after,
        refunds_after,
        CASE WHEN orders_before > 0 THEN ROUND(100.0 * refunds_before::numeric / orders_before, 2) ELSE NULL END AS return_rate_percent_before,
        CASE WHEN orders_after > 0  THEN ROUND(100.0 * refunds_after::numeric / orders_after, 2)  ELSE NULL END AS return_rate_percent_after,
        CASE
            WHEN orders_before > 0 AND orders_after > 0
            THEN ROUND( (100.0 * refunds_after::numeric / orders_after) - (100.0 * refunds_before::numeric / orders_before), 2)
            ELSE NULL
        END AS delta_pp
    FROM pivot
)
SELECT
    device,
    region,
    CASE
        WHEN LOWER(channel) IN ('paid', 'ads', 'cpi', 'cpp', 'cpa', 'sem', 'facebook_ads', 'google_ads') THEN 'Paid'
        WHEN LOWER(channel) IN ('organic', 'seo', 'direct', 'referral') THEN 'Organic'
        ELSE 'Other'
    END AS channel_group,
    return_rate_percent_before,
    return_rate_percent_after,
    delta_pp
FROM calc
ORDER BY device, region, channel_group;

-- Checkout funnel: add_to_cart → checkout_start → payment_success → refund

WITH release AS (
    SELECT DATE '2025-06-15' AS redesign_date
),
step AS (
    SELECT 
        fe.user_id,
        du.device,
        du.region,
        du.channel,
        CASE 
            WHEN fe.event_date < (SELECT redesign_date FROM release) THEN 'before'
            ELSE 'after'
        END AS period,
        MAX(CASE WHEN den.event_name = 'add_to_cart'      THEN 1 ELSE 0 END) AS s1_cart,
        MAX(CASE WHEN den.event_name = 'checkout_start' THEN 1 ELSE 0 END) AS s2_checkout,
        MAX(CASE WHEN den.event_name = 'payment_success'  THEN 1 ELSE 0 END) AS s3_payment,
        MAX(CASE WHEN den.event_name = 'refund'           THEN 1 ELSE 0 END) AS s4_refund
    FROM fact.fact_events fe
    JOIN dim.dim_event_name den 
        ON fe.event_name_id = den.event_name_id
    JOIN dim.dim_user du 
        ON fe.user_id = du.user_id
    WHERE den.event_name IN ('add_to_cart', 'checkout_start', 'payment_success', 'refund')
    GROUP BY fe.user_id, du.device, du.region, du.channel, period
),
metrics AS (
    SELECT
        device,
        region,
        channel,
        period,
        ROUND(100.0 * COUNT(*) FILTER (WHERE s2_checkout = 1) 
              / NULLIF(COUNT(*) FILTER (WHERE s1_cart = 1),0), 2) AS conv_cart_to_checkout_pct,
        ROUND(100.0 * COUNT(*) FILTER (WHERE s3_payment = 1) 
              / NULLIF(COUNT(*) FILTER (WHERE s2_checkout = 1),0), 2) AS conv_checkout_to_payment_pct,
        ROUND(100.0 * COUNT(*) FILTER (WHERE s4_refund = 1) 
              / NULLIF(COUNT(*) FILTER (WHERE s3_payment = 1),0), 2) AS refund_after_payment_pct
    FROM step
    GROUP BY device, region, channel, period
)
SELECT
    device,
    region,
    channel,
    'conv_cart_to_checkout_pct' AS metric,
    MAX(CASE WHEN period = 'before' THEN conv_cart_to_checkout_pct END) AS before,
    MAX(CASE WHEN period = 'after'  THEN conv_cart_to_checkout_pct END) AS after,
    MAX(CASE WHEN period = 'after'  THEN conv_cart_to_checkout_pct END) -
    MAX(CASE WHEN period = 'before' THEN conv_cart_to_checkout_pct END) AS diff
FROM metrics
GROUP BY device, region, channel
UNION ALL
SELECT
    device,
    region,
    channel,
    'conv_checkout_to_payment_pct',
    MAX(CASE WHEN period = 'before' THEN conv_checkout_to_payment_pct END),
    MAX(CASE WHEN period = 'after'  THEN conv_checkout_to_payment_pct END),
    MAX(CASE WHEN period = 'after'  THEN conv_checkout_to_payment_pct END) -
    MAX(CASE WHEN period = 'before' THEN conv_checkout_to_payment_pct END)
FROM metrics
GROUP BY device, region, channel
UNION ALL
SELECT
    device,
    region,
    channel,
    'refund_after_payment_pct',
    MAX(CASE WHEN period = 'before' THEN refund_after_payment_pct END),
    MAX(CASE WHEN period = 'after'  THEN refund_after_payment_pct END),
    MAX(CASE WHEN period = 'after'  THEN refund_after_payment_pct END) -
    MAX(CASE WHEN period = 'before' THEN refund_after_payment_pct END)
FROM metrics
GROUP BY device, region, channel
ORDER BY device, region, channel, metric;

-- D7 retention for new users in June 2025.
-- Users with reg_date in June who have any event exactly on day 7 after reg_date.

WITH users AS (
  	SELECT
    	user_id,
    	reg_date::date AS reg_date
  	FROM stg.stg_users_clean
),
qual_events AS (
  	SELECT
    	fe.user_id,
    	fe.event_date::date AS event_date,
		den.event_name
  	FROM fact.fact_events fe
	LEFT JOIN dim.dim_event_name den ON den.event_name_id = fe.event_name_id   
  	WHERE event_name IN ('add_to_cart', 'checkout_start', 'payment_success', 'refund')
),
cohorts AS (
  	SELECT
		'june' AS cohort,
        DATE '2025-06-01' AS start_date,
        DATE '2025-07-01' AS end_date
  	UNION ALL
	SELECT
		'may' AS cohort,
        DATE '2025-05-01' AS start_date,
        DATE '2025-06-01' AS end_date
),
cohort_users AS (
  	SELECT
		c.cohort,
		u.user_id,
		u.reg_date
  	FROM cohorts c
  	JOIN users u ON u.reg_date >= c.start_date AND u.reg_date <  c.end_date
),
cohort_retained AS (
  	SELECT
		DISTINCT cu.cohort, cu.user_id
  	FROM cohort_users cu
  	JOIN qual_events e ON e.user_id = cu.user_id AND e.event_date >= cu.reg_date AND e.event_date <  cu.reg_date + INTERVAL '7 days'
),
cohort_agg AS (
  	SELECT
    	c.cohort,
    	COUNT(DISTINCT cu.user_id) AS new_users_count,
    	COUNT(DISTINCT cr.user_id) AS retained_users_count,
      	CASE
			WHEN COUNT(DISTINCT cu.user_id) = 0 THEN 0
           	ELSE ROUND(100.0 * COUNT(DISTINCT cr.user_id)::numeric / COUNT(DISTINCT cu.user_id), 2)
      	END AS d7_retention
  	FROM cohort_users cu
  	LEFT JOIN cohort_retained cr ON cr.user_id = cu.user_id AND cr.cohort = cu.cohort
  	JOIN cohorts c ON c.cohort = cu.cohort
  	GROUP BY c.cohort
)
SELECT
  	MAX(CASE WHEN cohort = 'june' THEN new_users_count END) AS june_new_users_count,
  	MAX(CASE WHEN cohort = 'june' THEN retained_users_count END) AS june_retained_users_count,
  	MAX(CASE WHEN cohort = 'june' THEN d7_retention END) AS june_d7_retention,
	MAX(CASE WHEN cohort = 'may' THEN new_users_count END) AS may_new_users_count,
  	MAX(CASE WHEN cohort = 'may' THEN retained_users_count END) AS may_retained_users_count,
  	MAX(CASE WHEN cohort = 'may' THEN d7_retention END) AS may_d7_retention,
  	ROUND(100.0 * 
		(
    	COALESCE(MAX(CASE WHEN cohort = 'june' THEN d7_retention END),0)
    	- COALESCE(MAX(CASE WHEN cohort = 'may' THEN d7_retention END),0)
		) 
	/ COALESCE(MAX(CASE WHEN cohort = 'may' THEN d7_retention END),0), 2
  	) AS delta_d7_retention
FROM cohort_agg;

-- Share of problematic orders: refunded within 24 hours

WITH params AS (
    SELECT
        DATE '2025-06-15' AS redesign_date,
        INTERVAL '30 days' AS win
),
paid_orders AS (
    SELECT f.order_id, f.user_id, f.order_date
    FROM fact.fact_orders f
    JOIN dim.dim_order_status s ON f.status_id = s.status_id
    CROSS JOIN params p
    WHERE s.status_name IN ('paid', 'completed', 'delivered', 'success', 'approved')
      AND f.order_date >= p.redesign_date
      AND f.order_date <  p.redesign_date + p.win
),
refund_events AS (
    SELECT fe.user_id, fe.event_date AS refund_ts
    FROM fact.fact_events fe
    JOIN dim.dim_event_name den ON fe.event_name_id = den.event_name_id
    CROSS JOIN params p
    WHERE den.event_name IN ('refunded','refund','returned','chargeback')
      AND fe.event_date >= p.redesign_date
      AND fe.event_date <  p.redesign_date + p.win
),
first_refunds AS (
    SELECT
        po.order_id,
        po.user_id,
        po.order_date,
        MIN(re.refund_ts) AS refund_ts
    FROM paid_orders po
    LEFT JOIN refund_events re
           ON re.user_id = po.user_id
          AND re.refund_ts >= po.order_date
    GROUP BY po.order_id, po.user_id, po.order_date
)
SELECT
    COUNT(*) FILTER (WHERE refund_ts IS NOT NULL) AS total_refunded_orders,
    COUNT(*) FILTER (
        WHERE refund_ts IS NOT NULL
          AND refund_ts <= order_date + INTERVAL '24 hours'
    ) AS refunded_within_24h,
    ROUND(
        CASE WHEN COUNT(*) FILTER (WHERE refund_ts IS NOT NULL) > 0
             THEN 100.0 *
                  COUNT(*) FILTER (
                      WHERE refund_ts IS NOT NULL
                        AND refund_ts <= order_date + INTERVAL '24 hours'
                  )
                  / COUNT(*) FILTER (WHERE refund_ts IS NOT NULL)
             ELSE 0 END
    , 2) AS share_quick_refunds
FROM first_refunds;

--- Return Rate (RR) by cohort: Pre- vs. Post-Basket Redesign (May vs. June 2025)

WITH params AS (
  	SELECT
    	DATE '2025-06-15' AS redesign_date,
    	INTERVAL '30 days' AS rr_window_len
),
users AS (
  	SELECT
		user_id, 
		reg_date::date AS reg_date
  	FROM stg.stg_users_clean
)
--cohorts AS (
  	SELECT
    	u.user_id,
    	u.reg_date,
    	CASE
      		WHEN u.reg_date >= DATE '2025-05-15' AND u.reg_date < DATE '2025-06-01' THEN 'from_15_may_2025'
      		WHEN u.reg_date >= p.redesign_date AND p.redesign_date < DATE '2025-07-01' THEN 'post_update'
      		ELSE NULL
    	END AS cohort
  	FROM users u
  	CROSS JOIN params p
),
cohort_users AS (
  	SELECT 
		DISTINCT user_id, reg_date, cohort
  	FROM cohorts
  	WHERE cohort IN ('from_15_may_2025', 'post_update')
),
refunds_in_window AS (
  	SELECT
		DISTINCT cu.user_id, cu.cohort
  	FROM cohort_users cu
  	JOIN fact.fact_events e ON e.user_id = cu.user_id
  	CROSS JOIN params p
  	WHERE e.event_name_id = 3
    AND e.event_date::timestamp >= cu.reg_date::timestamp
    AND e.event_date::timestamp <  cu.reg_date::timestamp + p.rr_window_len
)
SELECT
  	COUNT(*) FILTER (WHERE cohort = 'from_15_may_2025') AS from_15_may_users,
  	COUNT(r.user_id) FILTER (WHERE cohort = 'from_15_may_2025') AS from_15_may_refunded_users,
  	ROUND(100.0 * (
    	COALESCE(
      	(COUNT(r.user_id) FILTER (WHERE cohort = 'from_15_may_2025'))::numeric
      	/ NULLIF(COUNT(*) FILTER (WHERE cohort = 'from_15_may_2025'), 0)
    	, 0)
	), 2)
  	AS from_15_may_rr,

  	COUNT(*) FILTER (WHERE cohort = 'post_update') AS post_users,
  	COUNT(r.user_id) FILTER (WHERE cohort = 'post_update') AS post_refunded_users,
  	ROUND(100.0 * (
    	COALESCE(
      	(COUNT(r.user_id) FILTER (WHERE cohort = 'post_update'))::numeric
      	/ NULLIF(COUNT(*) FILTER (WHERE cohort = 'post_update'), 0)
    	, 0)
	), 2)
  	AS post_rr,
	
  	ROUND(
		100.0 * (
		(
    	COALESCE(
      	(COUNT(r.user_id) FILTER (WHERE cohort = 'post_update'))::numeric
      	/ NULLIF(COUNT(*) FILTER (WHERE cohort = 'post_update'), 0)
    	, 0)
		)
    	/ 
		(
		COALESCE(
      	(COUNT(r.user_id) FILTER (WHERE cohort = 'from_15_may_2025'))::numeric
      	/ NULLIF(COUNT(*) FILTER (WHERE cohort = 'from_15_may_2025'), 0)
    	, 0)
		) - 1.0), 2)
  	AS delta_rr	
FROM cohort_users cu
LEFT JOIN refunds_in_window r USING (user_id, cohort);

-- Anti-join: drop-off among users who added to cart but did not complete payment 

WITH release AS (
    SELECT DATE '2025-06-15' AS redesign_date
),
cart_users AS (
	SELECT DISTINCT
    	fe.user_id,
    	du.device,
    	du.region,
    	du.channel,
    	CASE
      		WHEN fe.event_date < (SELECT redesign_date FROM release) THEN 'before'
      		ELSE 'after'
    	END AS period
  	FROM fact.fact_events fe
  	JOIN dim.dim_event_name den ON fe.event_name_id = den.event_name_id
  	JOIN dim.dim_user du ON fe.user_id = du.user_id
  	WHERE den.event_name = 'add_to_cart'
),
buyers AS (
  	SELECT DISTINCT
    	fe.user_id,
    	du.device,
    	du.region,
    	du.channel,
    	CASE
      		WHEN fe.event_date < (SELECT redesign_date FROM release) THEN 'before'
      		ELSE 'after'
    	END AS period
  	FROM fact.fact_events fe
  	JOIN dim.dim_event_name den ON fe.event_name_id = den.event_name_id
  	JOIN dim.dim_user du ON fe.user_id = du.user_id
  	WHERE den.event_name = 'payment_success'
)
SELECT
 	c.period,
  	c.device,
  	c.region,
  	c.channel,
  	COUNT(DISTINCT c.user_id) AS users_dropoff,
  	COUNT(DISTINCT c.user_id) AS users_added_to_cart,
  	ROUND(100.0 * (
    	COUNT(DISTINCT c.user_id) / NULLIF(COUNT(DISTINCT c.user_id), 0)),
    	2
  	) AS dropoff_rate_from_cart
FROM cart_users c
LEFT JOIN buyers b ON c.user_id = b.user_id
 AND c.period = b.period
 AND c.device = b.device
 AND c.region = b.region
 AND c.channel = b.channel
WHERE b.user_id IS NULL
GROUP BY c.period, c.device, c.region, c.channel
ORDER BY c.period, c.device, c.region, c.channel;