WITH SRC_RAW_MOVIES AS (
    SELECT * FROM {{source('raw','movies')}}
)
SELECT * FROM SRC_RAW_MOVIES