{#
Transforms a BIGINT into a valid date in the format 'YYYYMMDD'.
In the tables, we see dates casted such as 20240325.0.
Unfortunately, we also see values such as 1.0 or 0.0 in the same table.
This macro checks what could be a date and casts it.

Rarely, you will also find dates with 6 digits (e.g. 240101.0).
For these cases, just change the date_len flag to 6.

Example:

Input:
| opbdc1      |
|-------------|
| 20240325.0  |
| 20240628.0  |
| 20240902.0  |
| 20241215.0  |
| 1.0         |   -- Invalid, as it doesn't follow the 8-digit format

{{ cast_to_date('opbdc1') }}

Output:
| parsed_date |
|-------------|
| 2024-03-25  |
| 2024-06-28  |
| 2024-09-02  |
| 2024-12-15  |
| NULL        |  -- Invalid format returns NULL

Arguments:
- `date_column` (string): The name of the column containing the BIGINT date value to be transformed into a `DATE`.
- `date_len` (numeric): The length of the dates you are parsing. By default it is 8-digits. But this could be 6.

Returns:
- A valid `DATE` in the format `YYYY-MM-DD` if the input matches the 6-digit (`YYMMDD`) or 8-digit (`YYYYMMDD`) format.
- `NULL` if the input does not match the 6-digit or 8-digit format.
#}
{% macro sfg_cast_to_date(date_column, date_len=8) %}

{% if date_len == 8 %}{% set date_format='%Y%m%d' %}
{% elif date_len == 6 %}{% set date_format='%y%m%d' %}
{% endif %}

IF(
    /*Validate the dates are in a valid format
    I would normally use a regex for this, but it conflicts with automate_dv*/
    LENGTH(CAST({{ date_column }} AS STRING)) = {{ date_len }},
    PARSE_DATE('{{ date_format }}', SAFE_CAST({{ date_column }} AS STRING)),
    NULL
)
{% endmacro %}
