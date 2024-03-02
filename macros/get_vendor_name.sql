{#
    Returns the vendor name
#}

{% macro get_vendor_name(vendor_id) -%}

    case {{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }}
        when 1 then 'Creative Mobile Technologies'
        when 2 then 'VeriFone Inc'
        else 'Empty'
    end

{%- endmacro %}