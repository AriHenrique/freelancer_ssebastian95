{{ config(
    materialized='incremental',
    unique_key='symbol',
    target_schema='ref_financial',
    target_table='profile',
    strategy='merge'
) }}
select * from {{ source('raw_financial', 'profile') }}



