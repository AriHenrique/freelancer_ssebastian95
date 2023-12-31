{{ config(
    materialized='incremental',
    partitioned_by=['year_date'],
    target_schema='ref_financial',
    target_table='historical_price_full',
    strategy='merge'
) }}
select *, EXTRACT(YEAR FROM CAST(date AS DATE)) AS year_date from {{ source('raw_financial', 'earning_calendar') }}
