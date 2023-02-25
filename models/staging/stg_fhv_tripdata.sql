{{ config(materialized='view') }}

with tripdata as 
(
  select * except (int64_field_0),   
  from {{ source('staging_fhv','tripdata_2019_non_partitioned') }}
)
select
    -- identifiers
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    sr_flag,
    dispatching_base_num,
    affiliated_base_number as affiliated_base_num
    
from tripdata



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
