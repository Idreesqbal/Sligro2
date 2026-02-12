-- Only latest arrived files are staged.
-- Add descriptions via Jinja
{% set description_columns = ['sales_class', 'forecast_exception', 'stock_exception'] %}

SELECT
    cdc.*,
    {% for column in description_columns %}
    {{ column }}_description.details AS {{ column }}_short_description,
    {{ column }}_description.omschrijving AS {{ column }}_long_description,
    {% endfor %}
FROM {{ source('slim4', 'cdc_article_attributes') }} AS cdc
{% for column in description_columns %}
LEFT JOIN {{ source('gcs_manual_imports', 'slim4_description_' ~ column) }} AS {{ column }}_description
ON cdc.{{ column }} = {{ column }}_description.code
{% endfor %}
{% if not should_full_refresh() %}
WHERE
    true
{{ latest_dt('rv_sat__forecast_article_attributes_cdc', var('slim4_start_date')) }}
{% endif %}
