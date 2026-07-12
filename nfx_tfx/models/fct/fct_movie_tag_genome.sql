WITH src_fct_movie_genome_tag AS (
    SELECT * FROM {{ref('dim_genome_movie_tag')}}
)
SELECT * FROM src_fct_movie_genome_tag