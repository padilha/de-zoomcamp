{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by Dispatching_base_num, Pickup_datetime, DropOff_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['Dispatching_base_num', 'Pickup_datetime', 'DropOff_datetime']) }} as tripid,
    Dispatching_base_num as dispatching_base_num,
    cast(PULocationID as integer) as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(Pickup_datetime as timestamp) as pickup_datetime,
    cast(DropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as integer) as is_shared

from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}