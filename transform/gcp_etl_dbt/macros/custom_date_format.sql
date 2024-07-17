{% macro custom_date_format(column_name) %}
  -- Example implementation, adjust based on actual logic
  CAST({{ column_name }} AS VARCHAR)
{% endmacro %}