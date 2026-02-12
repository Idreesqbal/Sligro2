{% macro sfg_cast_to_date_6_digits(date_column) %}
IF(
    REGEXP_CONTAINS(CAST({{ date_column }} AS STRING), r'^\d{6}$'),
    PARSE_DATE(
        '%Y%m%d',
        CONCAT(
            '20',
            CAST({{ date_column }} AS STRING)
        )
    ),
    NULL
)
{% endmacro %}
