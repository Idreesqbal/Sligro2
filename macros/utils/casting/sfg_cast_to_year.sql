{% macro sfg_cast_to_year(year_column) %}
    IF(
        {{ year_column }} > 0,
        EXTRACT(YEAR FROM PARSE_DATE('%y', CAST({{ year_column }} AS STRING) )),
        NULL
    )
{% endmacro %}
