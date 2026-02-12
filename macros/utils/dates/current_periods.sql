{%- macro current_periods(date_column) -%}
    {%- for period in ['YEAR', 'QUARTER', 'MONTH', 'WEEK', 'ISOWEEK', 'DAY'] %}
    EXTRACT({{ period }} FROM {{ date_column }}) = EXTRACT({{ period }} FROM CURRENT_DATE()) AS current_{{ period | lower }}{% if not loop.last %},{% endif %}
    {%- endfor -%}
{%- endmacro -%}
