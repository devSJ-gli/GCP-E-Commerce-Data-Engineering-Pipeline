CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.presentation.product_summary`
AS
SELECT
    p.product_id,
    p.product_category_name,
    pct.product_category_name_english,

    COUNT(DISTINCT oi.order_id) AS order_count,
    COUNT(oi.product_id) AS item_count,

    SUM(oi.price) AS product_revenue,
    SUM(oi.freight_value) AS freight_revenue,
    SUM(oi.item_total_value) AS total_revenue,

    AVG(oi.price) AS average_item_price,
    AVG(oi.freight_value) AS average_freight_value

FROM `<GCP_PROJECT_ID>.curated.products` AS p

LEFT JOIN `<GCP_PROJECT_ID>.curated.product_category_translation` AS pct
    ON p.product_category_name = pct.product_category_name

LEFT JOIN `<GCP_PROJECT_ID>.curated.order_items` AS oi
    ON p.product_id = oi.product_id

GROUP BY
    p.product_id,
    p.product_category_name,
    pct.product_category_name_english;