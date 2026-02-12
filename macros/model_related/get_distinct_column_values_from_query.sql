{#- Prevent querying of db in parsing mode.
This works because this macro does not create any new refs. -#}
-- noqa: disable=all
{%- macro get_distinct_column_values_from_query(query, column) -%}
    {%- if not execute %}
        {{ return([]) }}  {# Return an empty list if not executing #}
    {%- endif %}

    {%- set column_values_sql %}
    WITH cte AS (
        {{ query }}
    )
    SELECT DISTINCT
        {{ column }} AS value
    FROM cte
    {% endset %}

    {%- set results = run_query(column_values_sql) %}
    {%- set results_list = results.columns[0].values() %}

    {{ return(results_list) }}
{%- endmacro %}
