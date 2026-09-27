CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.sellers`
AS

SELECT
    seller_id,
    seller_zip_code_prefix,

    LOWER(TRIM(seller_city)) AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state

FROM
  `<GCP_PROJECT_ID>.staging.olist_sellers_dataset`;