{% set exception_fields = ['stock_exception', 'forecast_exception'] %}

WITH combined_exceptions AS (
    SELECT
        demand_forecast_hk,
        demand_forecast_bk,
        {% for field in exception_fields %}
            {{ field }},
            {{ field }}_long_description,
            {{ field }}_short_description,
        {% endfor %}
        load_datetime,
        record_source
    FROM {{ ref('bv_bdv__forecast__attributes_bs') }}
    UNION ALL
    SELECT
        demand_forecast_hk,
        demand_forecast_bk,
        {% for field in exception_fields %}
            {{ field }},
            {{ field }}_long_description,
            {{ field }}_short_description,
        {% endfor %}
        load_datetime,
        record_source
    FROM {{ ref('bv_bdv__forecast__attributes_cdc') }}
)

{% for field in exception_fields %}
    SELECT
        CONCAT(
            demand_forecast_bk,
            "||",
            "{{ field }}",
            "||",
            {{ field }}
        ) AS pk_exception_code,
        demand_forecast_hk,
        "{{ field }}" AS exception_type,
        {{ field }} AS exception_code,
        {{ field }}_long_description AS long_description,
        {{ field }}_short_description AS short_description,
        load_datetime,
        record_source
    FROM
        combined_exceptions
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}
