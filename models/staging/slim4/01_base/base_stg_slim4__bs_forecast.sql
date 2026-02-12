SELECT *
FROM {{ source('slim4', 'bs_forecast') }}
{% if not should_full_refresh() %}
WHERE
    true
{{ latest_dt('rv_sat__forecast_bs', var('slim4_start_date')) }}
{% endif %}
