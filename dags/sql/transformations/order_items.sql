CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.order_items`
AS

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,

    DATE(shipping_limit_date) AS shipping_limit_date_only,

    price,
    freight_value,

    price + freight_value AS item_total_value,

    CASE
        WHEN price > 0
        THEN SAFE_DIVIDE(freight_value, price) * 100
        ELSE NULL
    END AS freight_percentage

FROM
  `<GCP_PROJECT_ID>.staging.olist_order_items_dataset`;