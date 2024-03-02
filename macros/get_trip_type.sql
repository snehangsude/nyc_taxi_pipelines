{#
    Get the location where the record of the trip was kept
#}

{%- macro get_trip_type(trip_type) -%}

    case {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }}
        when 1 then 'Street hail'
        when 2 then 'Dipatch'
        else 'Empty'
    end

{%- endmacro %}