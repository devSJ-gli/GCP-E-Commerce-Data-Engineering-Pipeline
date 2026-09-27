CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.geolocation`
AS

SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,

    LOWER(TRIM(geolocation_city)) AS geolocation_city,
    UPPER(TRIM(geolocation_state)) AS geolocation_state

FROM
  `<GCP_PROJECT_ID>.staging.olist_geolocation_dataset`;