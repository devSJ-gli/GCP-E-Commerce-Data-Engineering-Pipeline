CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.presentation.sales_summary`
AS

SELECT
    o.purchase_year,
    o.purchase_month,
    o.order_status,

    COUNT(DISTINCT o.order_id) AS order_count,

    COUNT(*) AS item_count,

    SUM(oi.price) AS product_revenue,

    SUM(oi.freight_value) AS freight_revenue,

    SUM(oi.item_total_value) AS total_revenue,

    SAFE_DIVIDE(
        SUM(oi.item_total_value),
        COUNT(DISTINCT o.order_id)
    ) AS average_order_value

FROM
  `<GCP_PROJECT_ID>.curated.orders` AS o

JOIN
  `<GCP_PROJECT_ID>.curated.order_items` AS oi
ON
  o.order_id = oi.order_id

GROUP BY
    o.purchase_year,
    o.purchase_month,
    o.order_status;