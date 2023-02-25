{{ config(materialized='table') }}

with fhv_trips as (
    select *, 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_trips.pickup_datetime,
    fhv_trips.dropoff_datetime,
    fhv_trips.pickup_locationid,
    fhv_trips.dropoff_locationid,
    fhv_trips.dispatching_base_num,
    fhv_trips.affiliated_base_num,
    fhv_trips.sr_flag
from fhv_trips
inner join dim_zones as pickup_zone
on fhv_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trips.dropoff_locationid = dropoff_zone.locationid
