{{
    config(
    materialized = 'table'
)}}

WITH SRC_RAW_RATINGS AS (
    SELECT * FROM {{source('raw','ratings')}}
)
SELECT userId as user_id,
       movieId as movie_id,
       RATING,
       to_timestamp_ltz(timestamp) as rating_timestamp FROM SRC_RAW_RATINGS