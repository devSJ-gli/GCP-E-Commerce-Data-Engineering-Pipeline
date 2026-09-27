CREATE OR REPLACE TABLE
  `<GCP_PROJECT_ID>.curated.reviews`
AS

SELECT
    review_id,
    order_id,
    review_score,

    NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
    NULLIF(TRIM(review_comment_message), '') AS review_comment_message,

    review_creation_date,
    review_answer_timestamp

FROM
  `<GCP_PROJECT_ID>.staging.olist_order_reviews_dataset`;