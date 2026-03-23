with latest_ingestion as (
    select
        ingestion_id
    from {{ source('bronze', 'lotofacil_ingestion_history') }}
    order by ingested_at desc, ingestion_id desc
    limit 1
)

select history.*
from {{ source('bronze', 'lotofacil_ingestion_history') }} as history
inner join latest_ingestion as latest
    on history.ingestion_id = latest.ingestion_id
