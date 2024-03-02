{{ config(materialized="table") }}

with
    green_trip_data as (
        select *, 'Green' as service_type from {{ ref("stg_green_taxi_trips") }}
    ),
    yellow_trip_data as (
        select *, 'Yellow' as service_type from {{ ref("stg_yellow_taxi_trips") }}
    ),
    trips_unioned as (
        select *
        from green_trip_data
        union all
        select *
        from yellow_trip_data
    ),
    taxi_zones as (select * from {{ ref("dim_taxi_zones") }} where borough != 'Unknown')

select
    trips_unioned.trip_id
    ,trips_unioned.vendor_id
    ,trips_unioned.vendor_name
    ,trips_unioned.service_type
    ,trips_unioned.ratecode_id
    ,trips_unioned.ratecode_type
    ,trips_unioned.pu_location_id
    ,pickup_zone.borough as pickup_borough
    ,pickup_zone.zone as pickup_zone
    ,trips_unioned.do_location_id
    ,dropoff_zone.borough as dropoff_borough
    ,dropoff_zone.zone as dropoff_zone
    ,trips_unioned.passenger_count
    ,trips_unioned.server_connection
    -- ,trips_unioned.trip_type_description
    ,trips_unioned.payment_type
    ,trips_unioned.payment_type_description
    ,trips_unioned.pickup_datetime
    ,trips_unioned.dropoff_datetime
    ,trips_unioned.trip_distance
    ,trips_unioned.total_amount
    ,trips_unioned.addon_fees
    ,trips_unioned.fare_amount
    ,trips_unioned.extra
    ,trips_unioned.mta_tax
    ,trips_unioned.tip_amount
    ,trips_unioned.tolls_amount
    ,trips_unioned.improvement_surcharge
from trips_unioned
inner join
    taxi_zones as pickup_zone on trips_unioned.pu_location_id = pickup_zone.locationid
inner join
    taxi_zones as dropoff_zone on trips_unioned.do_location_id = dropoff_zone.locationid
