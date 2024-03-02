{{
    config(
        materialized='view'
    )
}}

with green_trip_data as (
    select 
        *
        ,row_number() over(partition by vendor_id, lpep_pickup_datetime) as rnum
    from
        {{ source('staging', 'green_taxi_trips') }}
    where
        vendor_id is not null
)

select 
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'lpep_pickup_datetime']) }} as trip_id

    -- integer
    ,{{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} as vendor_id
    ,{{ get_vendor_name("vendor_id") }} as vendor_name
    ,{{ dbt.safe_cast("ratecode_id", api.Column.translate_type("integer")) }} as ratecode_id
    ,{{ get_ratecode_type("ratecode_id") }} as ratecode_type
    ,{{ dbt.safe_cast("pu_location_id", api.Column.translate_type("integer")) }} as pu_location_id
    ,{{ dbt.safe_cast("do_location_id", api.Column.translate_type("integer")) }} as do_location_id
    ,{{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count
    ,{{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type
    ,{{ get_trip_type("trip_type")}} as trip_type_description
    ,coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }} , 0) as payment_type
    ,{{ get_payment_type("payment_type") }} as payment_type_description
    
    -- timestamp
    ,cast(lpep_pickup_datetime as timestamp) as pickup_datetime
    ,cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime

    -- numeric

    ,cast(trip_distance as numeric) as trip_distance
    ,cast(fare_amount as numeric) as fare_amount
    ,cast(extra as numeric) as extra
    ,cast(mta_tax as numeric) as mta_tax
    ,cast(tip_amount as numeric) as tip_amount
    ,cast(tolls_amount as numeric) as tolls_amount
    ,cast(ehail_fee as numeric) as ehail_fee
    ,cast(improvement_surcharge as numeric) as improvement_surcharge
    ,cast(total_amount as numeric) as total_amount
    ,store_and_fwd_flag as server_connection

    -- aggregration
    ,ROUND({{ dbt_utils.safe_add(["extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge"]) }}, 2) as addon_fees


from 
    green_trip_data
where
    rnum = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
    