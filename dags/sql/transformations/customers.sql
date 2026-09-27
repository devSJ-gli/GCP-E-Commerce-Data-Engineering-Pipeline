CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.customers`
AS

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,

    LOWER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state

FROM
  `<GCP_PROJECT_ID>.staging.olist_customers_dataset`;