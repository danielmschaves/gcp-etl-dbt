{% macro custom_date_format(column_name) %}
    CAST({{ column_name }} AS VARCHAR)
{% endmacro %}
