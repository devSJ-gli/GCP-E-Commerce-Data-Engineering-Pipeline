CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.payments`
AS

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,

    CASE
        WHEN payment_installments > 0
        THEN SAFE_DIVIDE(
            payment_value,
            payment_installments
        )
        ELSE NULL
    END AS payment_value_per_installment

FROM
  `<GCP_PROJECT_ID>.staging.olist_order_payments_dataset`;