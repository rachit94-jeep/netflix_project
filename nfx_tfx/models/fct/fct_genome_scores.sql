WITH src_fct_genome_score AS (
    SELECT * FROM {{ref('src_genome_scores')}}
)
SELECT movie_id,
       tag_id,
       ROUND(relevance,4) as relevance
       FROM src_fct_genome_score
       WHERE relevance > 0