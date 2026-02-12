{% macro national_holiday_current_data(model_name) %}
    {#
    The national holiday data sourced from a CSV file and it doesn't record the change data capture.
    This macro aims to prepare current national holiday data in the raw vault,
    and later it'll be used in the staging layer to detect deleted data.
    #}
    {# default empty table for national holiday data #}
    {% set empty_data %}
        SELECT
            CAST('9999-12-31' AS DATE) AS calendar_date_nk,
            '' AS holiday_name,
            ''  AS holiday_type,
            CAST('9999-12-31' AS DATE) AS holiday_start_date,
            CAST('9999-12-31' AS DATE) AS holiday_end_date,
            ''  AS country_region,
            ''  AS country,
            {{ var("end_date") }} AS valid_to
            LIMIT 0
    {% endset %}
    {% if execute %}
        {# Get database and schema used by the national holiday raw vault #}
        {% set model = graph['nodes'].values()
                        | selectattr("resource_type", "equalto", "model")
                        | selectattr('name', 'equalto', model_name) | first | default(none) %}
        {% if model is not none %}
            {# Check if the raw vault for national holiday already ran #}
            {% set relation = adapter.get_relation(
                                database=model.database,
                                schema=model.schema,
                                identifier=model.name) %}
            {% if relation %}
            {# Populate the current national holiday data from raw vault #}
                SELECT
                    calendar_date_nk,
                    holiday_name,
                    holiday_type,
                    holiday_start_date,
                    holiday_end_date,
                    country_region,
                    country,
                    valid_to
                FROM {{ relation }}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY calendar_date_nk, holiday_name, holiday_type, country_region, country
                    ORDER BY load_datetime DESC
                ) = 1
            {% else %}
            {{ empty_data }}
            {% endif %}
        {% else %}
            {{ empty_data }}
        {% endif %}
    {% endif %}
{% endmacro %}
