{{
    config(
        materialized='view'
    )
}}

with trip_data as (
    select 
        *
        ,row_number() over(partition by vendor_id, tpep_pickup_datetime) as rnum
    from
        {{ source('staging', 'yellow_taxi_trips') }}
    where
        vendor_id is not null
)

select 
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'tpep_pickup_datetime']) }} as trip_id

    -- integer
    ,{{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} as vendor_id
    ,{{ get_vendor_name("vendor_id") }} as vendor_name
    ,{{ dbt.safe_cast("ratecode_id", api.Column.translate_type("integer")) }} as ratecode_id
    ,{{ get_ratecode_type("ratecode_id") }} as ratecode_type
    ,{{ dbt.safe_cast("pu_location_id", api.Column.translate_type("integer")) }} as pu_location_id
    ,{{ dbt.safe_cast("do_location_id", api.Column.translate_type("integer")) }} as do_location_id
    ,{{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count
    ,coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }} , 0) as payment_type
    ,{{ get_payment_type("payment_type") }} as payment_type_description
    
    -- timestamp
    ,cast(tpep_pickup_datetime as timestamp) as pickup_datetime
    ,cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime

    -- numeric
    ,1 as trip_type
    ,cast(trip_distance as numeric) as trip_distance
    ,cast(fare_amount as numeric) as fare_amount
    ,cast(extra as numeric) as extra
    ,cast(mta_tax as numeric) as mta_tax
    ,cast(tip_amount as numeric) as tip_amount
    ,cast(tolls_amount as numeric) as tolls_amount
    ,cast(0 as numeric) as ehail_fee
    ,cast(improvement_surcharge as numeric) as improvement_surcharge
    ,cast(total_amount as numeric) as total_amount
    ,store_and_fwd_flag as server_connection

    --aggregration
    ,{{ dbt_utils.safe_add(["extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge"]) }} as addon_fees

from 
    trip_data
where
    rnum = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
    