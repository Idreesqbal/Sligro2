{% macro cast_to_datetime(date_column, time_column) %}
    {# Create a datetime field based on the input of:a Date column (bigint) +  Time column (bigint).#}
DATETIME( IF({{ date_column }} > 0, PARSE_DATE('%Y%m%d', CAST({{ date_column }} AS STRING)), NULL) {# Convert the integer to a date using PARSE_DATE to a date format YYYY-MM-DD #}
      , IF({{ time_column }} > 0, PARSE_TIME('%H%M%S', LPAD(CAST({{ time_column }} as STRING), 6, '0')), PARSE_TIME('%H', '0'))) {# Convert the integer to a time using PARSE_Time to a time format Hours-Minutes-Seconds #}
    {# Using the function lpad to create a prefix 0 when time-column has less then 6 characters #}
{% endmacro %}
