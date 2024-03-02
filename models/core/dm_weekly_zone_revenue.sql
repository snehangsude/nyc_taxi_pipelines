{{
    config(
        materialized='table'
    )
}}

with weekly_revenue as (
    select 
        *
    from 
        {{ ref('fct_trips_data') }}
)

select
    service_type
    ,pickup_zone as revenue_zone
    ,{{ dbt.date_trunc("week", "pickup_datetime") }} as revenue_week
    ,sum(fare_amount) as revenue_fare_amount
    ,sum(addon_fees) as revenue_addon_fees
    ,sum(extra) as revenue_weekly_extra
    ,sum(mta_tax) as revenue_weekly_mta_tax
    ,sum(tip_amount) as revenue_weekly_tip_amount
    ,sum(tolls_amount) as revenue_weekly_tolls_amount
    ,sum(improvement_surcharge) as revenue_weekly_improvement_surcharge
    ,sum(total_amount) as revenue_weekly_total_amount
    ,count(trip_id) as total_weekly_trips
    ,avg(passenger_count) as avg_weekly_passenger_count
    ,avg(trip_distance) as avg_weekly_trip_distance
from
    weekly_revenue
group by
    service_type, revenue_zone, revenue_week