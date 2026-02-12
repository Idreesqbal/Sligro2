{% macro previous_dateparts(period_name, date_column, year_column, datepart_column) %}
    {#
    Identify the last complete period, month, quarter, and week.
    The concatenation with the year is needed in case the last full date parts fall in the previous year.
    #}
    {% set previous_year = year_column ~ ' - 1' %}

    {% if period_name == 'week' %}
        {% set end_of_year_period = 'EXTRACT(ISOWEEK FROM DATE_SUB(DATE_TRUNC(' ~ date_column ~ ', WEEK (MONDAY)), INTERVAL 1 WEEK))' %}
    {%  elif period_name == 'month' %}
        {% set end_of_year_period = 12 %}
    {%- elif period_name == 'quarter' %}
        {% set end_of_year_period = 4 %}
    {%  elif period_name == 'four_week_period' %}
        {% set end_of_year_period = 13 %}
    {% endif %}

    CASE
        WHEN {{ datepart_column }}  = 1
        THEN CONCAT({{ previous_year }}, {{ end_of_year_period }})
    ELSE CONCAT({{ year_column }}, {{ datepart_column }}  - 1)
    END

{% endmacro %}
