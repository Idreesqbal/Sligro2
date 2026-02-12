{%- macro sfg_convert_to_snake_case(column_name) -%}
    -- noqa: disable=all
    {% set re = modules.re %}
    {%- set is_snake_case = re.match("^[a-z0-9_]+$", column_name) -%}

    {%- if is_snake_case -%}
        {{ return(column_name) }}  {# If it is already snake_case, return as is #}
    {%- else -%}
        {% set snake_case_name = re.sub("(?<!^)([A-Z][a-z]|(?<=[a-z])[^a-z]|(?<=[A-Z])[0-9_])", "_\\1", column_name).lower() %}
        {{ return(snake_case_name | replace('-', '_') | replace('__', '_')) }}  {# Handle double underscores #}
    {%- endif -%}
{%- endmacro %}
