{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail',
        unique_key = ['user_id','movie_id']
    )
}}

WITH src_fct_ratings_inc AS(
    SELECT * FROM {{ref('src_ratings')}}
)
SELECT user_id,
       movie_id,
       rating,
       rating_timestamp
       FROM src_fct_ratings_inc
       WHERE rating IS NOT NULL

{% if is_incremental() %}
    AND rating_timestamp > (SELECT MAX(rating_timestamp) FROM {{this}})
{% endif %}