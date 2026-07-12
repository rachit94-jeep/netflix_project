WITH src_ratings AS (
    SELECT DISTINCT user_id FROM {{ref('src_ratings')}}
),
src_tags AS (
    SELECT DISTINCT user_id FROM {{ref('src_tags')}}
)
SELECT * FROM src_ratings
UNION
SELECT * FROM src_tags