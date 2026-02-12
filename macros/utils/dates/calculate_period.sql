{% macro calculate_period(date_column, iso_week = True) %}
    {% set date_part = 'ISOWEEK' if iso_week == True else 'WEEK' %}
    CEIL(EXTRACT({{ date_part }} FROM {{ date_column }}) / 4.0)
{% endmacro %}
