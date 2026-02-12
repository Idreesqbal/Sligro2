{#
Transforms a BIGINT representing time in the format HHMM or HMM into a SQL `TIME`.
In the tables, we see dates casted such as 2359.0 or 900.0
Unfortunately, we also see values such as 1.0 or 0.0 in the same table.
This macro checks what could be a time and casts it.

Example:

Input:
| opet1      |
|------------|
| 2359.0     |
| 900.0      |  -- Equivalent to 09:00
| 1.0        |  -- Invalid
| 145.0      |  -- Equivalent to 01:45
| 0.0        |  -- Invalid

{{ cast_to_time('opetc1') }}

Output:
| parsed_time |
|-------------|
| 23:59:00    |
| 09:00:00    |
| NULL        |  -- Invalid format returns NULL
| 01:45:00    |
| NULL        |  -- Invalid format returns NULL

Arguments:
- `time_column` (string): The name of the column containing the BIGINT time value to be transformed into a `TIME`.

Returns:
- A valid `TIME` in the format `HH:MM:SS` if the input is a valid time with 3 or 4 digits.
- `NULL` if the input does not match the valid time format.

#}

{% macro stg_cast_to_time(time_column) %}
    IF(
        -- Validate the time is in a valid 3 or 4 digit format (HMM or HHMM)
        REGEXP_CONTAINS(CAST({{ time_column }} AS STRING), r'\d{3,4}'),
        TIME(
            -- Extract the hour from the first 1 or 2 digits
            CAST(FLOOR({{ time_column }} / 100) AS INT64),
            -- Extract the minutes from the last 2 digits
            CAST(MOD({{ time_column }}, 100) AS INT64),
            -- Always set seconds to 00
            00
        ),
        NULL
    )
{% endmacro %}
