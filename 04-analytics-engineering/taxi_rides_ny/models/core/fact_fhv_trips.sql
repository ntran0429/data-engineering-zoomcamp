{{
    config(
        materialized='table'
    )
}}


-- Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones. 
-- Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries 
-- for pickup and dropoff locations. Run the dbt model without limits (is_test_run: false).
with fhv_tripdata as (
    select *, 
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv.*,
    pickup_zones.borough as pickup_borough, 
    pickup_zones.zone as pickup_zones, 
    dropoff_zones.borough as dropoff_borough, 
    dropoff_zones.zone as dropoff_zones,  
from fhv_tripdata as fhv
join dim_zones as pickup_zones
on fhv.PUlocationID = pickup_zones.locationid
join dim_zones as dropoff_zones
on fhv.DOlocationID = dropoff_zones.locationid
-- the two joins make sure we only keep records with both pickup zone and dropoff zone recorded