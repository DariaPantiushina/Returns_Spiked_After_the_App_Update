# Returns Spiked After the App Update

**Business context**: In June 2025, the mobile marketplace team launched a redesigned shopping cart (new checkout UI). Two weeks later, the refund rate had risen from 7.08% to 15.69%.

The product team raised **two questions**:

1) Is this a bug, random fluctuation, or a real trend?

2) Which users specifically started returning purchases more frequently?

**Business questions**:

1) How did the Return Rate (RR) change before and after the release?

2) Which segments show the strongest growth (by device, region, channel)?

3) Is the increase in returns associated with the decline in payment conversion?

4) Did the UI update have an impact on Day-7 Retention (D7)?

## Data Mart Schema

The architecture of the data mart includes **four layers**:

1. stg_ (**Staging**)

- **stg_orders_raw**(order_id, user_id, order_date, amount, status);
  
- **stg_users_raw**(user_id, reg_date, device, region, channel);

- **stg_events_raw**(event_id, user_id, event_date, event_name)

Includes light data cleaning (data type conversions, removal of explicit duplicates, etc.), implemented in: **stg_orders_clean**, **stg_users_clean**, **stg_events_clean**.

2. dim_ (**Dimensions**)

- **dim_date**(date_key, iso_dow, day_of_month, week_of_year, month, month_name, quarter, year, week_start, month_start, quarter_start, year_start, is_weekend);

- **dim_user**(user_id, region, device, channel);

- **dim_order_status**(status_id, status_name);

- **dim_event_name**(event_name_id, event_name)

Contains reference tables for consistent dimension data.

3. fact_ (**Facts**)

- **fact_orders**(user_id, order_id, amount, order_date, status_id);

- **fact_events**(user_id, event_date, event_name_id)

Contains fact tables with transactional and event data.

4. marts_ (**Analytics Marts**)

- **marts_kpi_daily**(date, orders, refunds, return_rate, conv_to_payment, conv_cart_to_checkout_pct, conv_checkout_to_payment_pct, refund_after_payment_pct, users_cart, users_checkout, users_payment, users_refund, d7_retention);

**Optimizing the SQL query** with **indexes** and **materialized** reference tables (status_map, event_map) reduced execution time from 16.132s to 0.402s - a 97.5% improvement.

- **marts_segments**(date, device, region, channel, orders, refunds, return_rate)

Contains aggregated summary tables for reporting and visualization.

## SQL-analysis

1) Return Rate (RR) before vs after 2025-06-15 (UI release);

2) Return Rate (RR) before vs after 2025-06-15 (UI release). A/B comparison segments by: Android vs iOS (device type); EU vs US vs ASIA (region); Paid vs Organic (user's channel);

3) Checkout funnel: add_to_cart → checkout_start → payment_success → refund;

4) D7 retention for new users in June 2025 (users with reg_date in June who have any event exactly on day 7 after reg_date);

5) Proportion of orders refunded within 24 hours;

6) Return Rate (RR) by cohort: Pre- vs. Post-Basket Redesign (May vs. June 2025);

7) Anti-join: drop-off among users who added to cart but did not complete payment

## Python-analysis: text clustering for customer comments

Customer comments from the two weeks after launch were preprocessed (tokenized, lemmatized, stop words removed) and vectorized using TF-IDF. A K-Means model with 3 clusters was selected as optimal based on inertia and silhouette performance:

- Cluster 0 (checkout usability & design confusion) - the dominant issue involves navigation friction and missing checkout controls;

- Cluster 1 (customer support) - support interactions are largely positive;

- Cluster 2 (cart functionality breakdown) - strong indications of a technical regression in cart behavior

Overall conclusion → the text analysis points to a real, design-related and functional issue: most negative feedback centers on the checkout and cart flow.

## Creating a Dashboard in Power BI

["Dashboard"](power_bi/dashboard.pdf), also available via public link: https://app.powerbi.com/groups/me/reports/a637e7c4-fbc1-4969-80c6-93bcb5ebfc10?ctid=8d7391b6-6d66-4625-ada5-ff437c1a0e9c&pbi_source=linkShare):

1) KPI Blocks: Orders, Refunds, Return Rate (RR%), Conversion to Payment (Conv%), Day-7 Retention (D7);

2) Daily Return Rate (RR) - significant increase observed after June 15;

3) Bar chart: Return Rate (RR) by device - Android +3.79p.p., iOS -0.68p.p.;

4) Highlighted Table: Return Rate (RR) by region;

5) Checkout funnel comparison: May vs. June (UI update released in June);

6) Table: Segments with the greatest increase in returns following the basket redesign

## Findings & Business Insights & Recommendations

1) The overall Return Rate (RR) increased from 7.08% to 15.69% (+121.61%);

2) Android EU Paid showed the highest increase, rising by +44.34 percentage points. iOS remained almost unchanged, suggesting the issue is specific to Android;

3) Checkout-to-payment conversion fell by 41.95 percentage points in the same segment (Android EU Paid);

4) The D7 retention of new users decreased from 54.09% in May to 50.80% in June;

5) 12.78% of returns occur within the first 24 hours, suggesting a potential UX bug;

6) Recommendations:

- apply an Android UI hotfix;

- test an alternative checkout via A/B testing

7) Risks: rollback could lead to decreased conversion rates
