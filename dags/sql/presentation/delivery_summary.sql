CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.presentation.delivery_summary`
AS
SELECT
    order_id,
    customer_id,
    order_status,

    purchase_date,

    order_delivered_customer_date,
    order_estimated_delivery_date,

    delivery_days,
    delivery_delay_days,

    CASE
        WHEN delivery_delay_days IS NULL THEN NULL
        WHEN delivery_delay_days > 0 THEN 'late'
        WHEN delivery_delay_days < 0 THEN 'early'
        ELSE 'on_time'
    END AS delivery_performance

FROM `<GCP_PROJECT_ID>.curated.orders`;