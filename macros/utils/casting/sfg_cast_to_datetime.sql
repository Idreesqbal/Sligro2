{% macro stg_cast_to_datetime(date_column, time_column) %}
    DATETIME(
        {{ cast_to_date(date_column) }},
        {{ cast_to_time(time_column) }}
    )
{% endmacro %}
