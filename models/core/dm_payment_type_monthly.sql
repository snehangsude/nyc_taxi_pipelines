{{
    config(
        materialized='table'
    )
}}

with payment_type_monthly_revenue as (
    select 
        *
    from 
        {{ ref('fct_trips_data') }}
)

select
    service_type
    ,payment_type
    ,{{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month
    ,sum(fare_amount) as revenue_fare_amount
    ,sum(addon_fees) as revenue_addon_fees
    ,sum(extra) as revenue_monthly_extra
    ,sum(mta_tax) as revenue_monthly_mta_tax
    ,sum(tip_amount) as revenue_monthly_tip_amount
    ,sum(tolls_amount) as revenue_monthly_tolls_amount
    ,sum(improvement_surcharge) as revenue_monthly_improvement_surcharge
    ,sum(total_amount) as revenue_monthly_total_amount
    ,count(trip_id) as total_monthly_trips
    ,avg(passenger_count) as avg_monthly_passenger_count
    ,avg(trip_distance) as avg_monthly_trip_distance
from
    payment_type_monthly_revenue
group by
    service_type, payment_type, revenue_month