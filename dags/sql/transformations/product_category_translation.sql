CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.product_category_translation`
AS

SELECT
    string_field_0 AS product_category_name,
    string_field_1 AS product_category_name_english

FROM
  `<GCP_PROJECT_ID>.staging.product_category_name_translation`;