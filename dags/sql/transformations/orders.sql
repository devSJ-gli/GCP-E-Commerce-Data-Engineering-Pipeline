CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.orders`
AS

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    DATE(order_purchase_timestamp) AS purchase_date,

    EXTRACT(YEAR FROM order_purchase_timestamp) AS purchase_year,

    EXTRACT(MONTH FROM order_purchase_timestamp) AS purchase_month,

    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN DATE_DIFF(
            DATE(order_delivered_customer_date),
            DATE(order_purchase_timestamp),
            DAY
        )
        ELSE NULL
    END AS delivery_days,

    CASE
        WHEN order_delivered_customer_date IS NOT NULL
             AND order_estimated_delivery_date IS NOT NULL
        THEN DATE_DIFF(
            DATE(order_delivered_customer_date),
            DATE(order_estimated_delivery_date),
            DAY
        )
        ELSE NULL
    END AS delivery_delay_days

FROM
  `<GCP_PROJECT_ID>.staging.olist_orders_dataset`;