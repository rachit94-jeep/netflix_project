
WITH SRC_DIM_MOVIES AS (
    SELECT * FROM {{ref('src_movies')}}
)

SELECT movies_id,
       INITCAP(TRIM(title)) as movie_title,
       {{ split_array('genres') }} as genre_array,
       genres
FROM SRC_DIM_MOVIES