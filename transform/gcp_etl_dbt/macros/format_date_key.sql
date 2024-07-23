{% macro format_date_key(date_column) %}
    CONCAT(
        CAST(EXTRACT(YEAR FROM {{ date_column }}) AS VARCHAR), 
        LPAD(CAST(EXTRACT(MONTH FROM {{ date_column }}) AS VARCHAR), 2, '0'), 
        LPAD(CAST(EXTRACT(DAY FROM {{ date_column }}) AS VARCHAR), 2, '0')
    )
{% endmacro %}
