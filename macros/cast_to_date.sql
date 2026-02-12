{% macro cast_to_date(date_column) %}
    {#
    Parse a date string in the YYMMDD format into a valid DATE object.
    Ignores zero values by returning NULL.
    #}
    IF({{ date_column }} > 0, PARSE_DATE('%y%m%d', LPAD(CAST({{ date_column }} as STRING), 6, '0')), NULL)
{% endmacro %}
