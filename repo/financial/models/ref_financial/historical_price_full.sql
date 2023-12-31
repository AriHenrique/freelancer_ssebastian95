{{ config(
    materialized='incremental',
    partitioned_by=['year_search_date'],
    target_schema='ref_financial',
    target_table='historical_price_full',
    strategy='merge'
) }}
select *, EXTRACT(YEAR FROM CAST(search_date AS DATE)) AS year_search_date from {{ source('raw_financial', 'historical_price_full') }}