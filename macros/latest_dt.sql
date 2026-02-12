{%- macro latest_dt(model_name, default_date) -%}
    {# model_name is a satellite name where the data should be loaded into #}
    {%- if execute -%}
        {# Check if the model_name is valid #}
        {%- set rows = [] -%}
        {%- set model = graph['nodes'].values()
                        | selectattr("resource_type", "equalto", "model")
                        | selectattr('name', 'equalto', model_name) | first | default(none) -%}
        {%- if model is not none and not flags.FULL_REFRESH -%}
            {# Check if the raw vault for the model already ran, otherwise use the default value #}
            {%- set relation = adapter.get_relation(
                                database=model.database,
                                schema=model.schema,
                                identifier=model.name) -%}
            {%- if relation -%}
            {# If the model exists, get the latest load_date #}
                {%- set query -%}
                    SELECT MAX(DATE(load_datetime)) FROM {{ relation }}
                {%- endset -%}
                {%- set result = run_query(query) -%}
                {%- if result -%}
                    {%- set rows = result.columns[0].values() -%}
                {%- endif -%}
            {%- endif -%}
        {% endif %}

        {# Return the latest load_datetime #}
        AND dt >= DATE(COALESCE({{ "'"~rows[0]~"'" if (rows | length > 0 and rows[0] is not none) else 'NULL' }}, '{{ default_date }}'))
    {%- endif -%}
{%- endmacro -%}
