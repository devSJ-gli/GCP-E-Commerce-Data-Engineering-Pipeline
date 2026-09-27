CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.presentation.customer_summary`
AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,

    COUNT(DISTINCT o.order_id) AS order_count,

    MIN(o.purchase_date) AS first_purchase_date,
    MAX(o.purchase_date) AS last_purchase_date,

    SUM(oi.item_total_value) AS total_revenue,

    SAFE_DIVIDE(
        SUM(oi.item_total_value),
        COUNT(DISTINCT o.order_id)
    ) AS average_order_value,

    COUNT(DISTINCT oi.product_id) AS distinct_products_purchased

FROM `<GCP_PROJECT_ID>.curated.customers` AS c

LEFT JOIN `<GCP_PROJECT_ID>.curated.orders` AS o
    ON c.customer_id = o.customer_id

LEFT JOIN `<GCP_PROJECT_ID>.curated.order_items` AS oi
    ON o.order_id = oi.order_id

GROUP BY
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state;