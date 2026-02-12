{% macro period_year_combination(period_name, year_column, date_part_column) %}
    {#
    Combine year and date period (week/four week period/quarter/month/year)
    with zero padding added to enable sortable values.
    #}
    {% if period_name == 'quarter' %}
        CONCAT({{ year_column }}, '-Q', {{ date_part_column }})
    {% else %}
        CONCAT({{ year_column }}, '-', LPAD(CAST({{ date_part_column }} AS STRING), 2, '0'))
    {% endif %}
{% endmacro %}
