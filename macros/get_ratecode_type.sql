{#
    Return the type of trip based on the rate code
#}

{% macro get_ratecode_type(ratecode_id) -%}

    case {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("integer")) }}
        when 1 then 'Standard rate'
        when 2 then 'JFK'
        when 3 then 'Newark'
        when 4 then 'Nassau or Westchester'
        when 5 then 'Negotiated fare'
        when 6 then 'Group ride'
        else 'Empty'
    end

{%- endmacro %}