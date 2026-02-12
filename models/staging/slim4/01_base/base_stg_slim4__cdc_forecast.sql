SELECT *
FROM {{ source('slim4', 'cdc_forecast') }}
{% if not should_full_refresh() %}
WHERE
    true
{{ latest_dt('rv_sat__forecast_cdc', var('slim4_start_date')) }}
{% endif %}
