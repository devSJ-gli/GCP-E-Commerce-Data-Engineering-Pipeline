CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.products`
AS

SELECT
    product_id,
    product_category_name,

    product_name_lenght AS product_name_length,
    product_description_lenght AS product_description_length,

    product_photos_qty,
    product_weight_g,

    product_length_cm,
    product_height_cm,
    product_width_cm,

    CASE
        WHEN product_length_cm IS NOT NULL
             AND product_height_cm IS NOT NULL
             AND product_width_cm IS NOT NULL
        THEN
            product_length_cm
            * product_height_cm
            * product_width_cm
        ELSE NULL
    END AS product_volume_cm3

FROM
  `<GCP_PROJECT_ID>.staging.olist_products_dataset`;