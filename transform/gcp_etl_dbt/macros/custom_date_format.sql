{% macro custom_date_format(date_column) %}
    CAST(STRFTIME('%Y%m%d', {{ date_column }}) AS INTEGER)
{% endmacro %}
