{% snapshot tag_snapshot %}

{{
    config(
        target_schema = 'SNAPSHOTS',
        unique_key = ['user_id','movie_id','tag'],
        strategy = 'timestamp',
        updated_at = 'tag_timestamp',
        invalidate_hard_deletes = True

    )
}}

SELECT 
       {{dbt_utils.generate_surrogate_key(['user_id','movie_id','tag'])}} as gen_key,
       user_id,
       movie_id,
       tag,
       tag_timestamp
    FROM {{ref('src_tags')}} LIMIT 100

{% endsnapshot %}