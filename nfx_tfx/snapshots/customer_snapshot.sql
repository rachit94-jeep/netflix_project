{% snapshot customer_snapshot %}

{{
config(

target_schema='SNAPSHOTS',

unique_key='customer_id',

strategy='timestamp',

updated_at='updated_at'

)

}}

select *

from {{ source('analytics','customers_scd') }}

{% endsnapshot %}