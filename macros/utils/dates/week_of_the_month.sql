{% macro week_of_the_month(date_column) %}
    EXTRACT(ISOWEEK FROM {{ date_column }}) - EXTRACT(ISOWEEK FROM DATE_TRUNC({{ date_column }}, MONTH)) + 1
{% endmacro %}
