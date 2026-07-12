{{
    config(
        materialized = 'ephemeral'
    )
}}

WITH movies AS (
    SELECT * FROM {{ref('dim_movies')}}
),
genome_tags AS (
    SELECT * FROM {{ref('dim_genome_tags')}}
),
genome_score AS (
    SELECT * FROM {{ref('fct_genome_scores')}}
)
SELECT m.movies_id as movie_id,
       m.movie_title as title,
       gt.tag_id as tag_id,
       gt.tag as tag,
       gs.relevance as relevance
        FROM movies as m 
        LEFT JOIN genome_score as gs
        ON m.movies_id = gs.movie_id
        LEFT JOIN genome_tags as gt
        ON gs.tag_id = gt.tag_id