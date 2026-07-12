WITH SRC_RAW_TAGS AS (
    SELECT * FROM MOVIELENS.RAW.RAW_TAGS
)
SELECT userId as user_id,
       movieId as movie_id,
       tag,
       to_timestamp_ltz(timestamp) as tag_timestamp
       FROM SRC_RAW_TAGS