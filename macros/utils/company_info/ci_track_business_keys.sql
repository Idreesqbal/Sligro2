{# ci_filter_deleted_rows: Filter out deleted rows by selecting the keys that appears in the latest loaded data
   rv_hub_link: The hub or link table in the raw vault
   rv_xts: The extended tracking satellite table in the raw vault
   pk: The primary key to join hub and xts
   tracked_sat_name: The name of satellite where the deleted row should be filtered
#}
{% macro ci_filter_deleted_rows(rv_hub_link, rv_xts, pk, tracked_sat_name) %}
    SELECT
        hub_link.*,
        xts.hashdiff
    FROM {{ rv_hub_link }} AS hub_link
    INNER JOIN
        {{ rv_xts }} AS xts
        ON hub_link.{{ pk }} = xts.{{ pk }}
    WHERE xts.satellite_name = {{ "'"~tracked_sat_name~"'" }}
    AND DATE(xts.load_datetime) = {{ ci_get_latest_xts_ldt(rv_xts, tracked_sat_name) }}
{% endmacro %}

{# ci_get_latest_xts_ldt: Get the latest load_datetime from a tracking satellite (XTS) #}
{%- macro ci_get_latest_xts_ldt(rv_xts, tracked_sat_name) -%}
    {%- set latest_ldt = none -%}
    {%- if execute -%}
        {%- set query -%}
            SELECT MAX(load_datetime) AS max_ldt
            FROM {{ rv_xts }}
            WHERE satellite_name = {{ "'"~tracked_sat_name~"'" }}
        {%- endset -%}
        {%- set result = run_query(query) -%}
        {%- if result and result.columns and result.rows -%}
            {%- set latest_ldt = result.rows[0][0] -%}
        {%- endif -%}
    {%- endif -%}
    DATE(COALESCE({{ "'"~latest_ldt~"'" if (latest_ldt is not none) else 'NULL' }}, '{{ var("company_info_start_date") }}'))
{%- endmacro -%}

{# ci_track_business_key_presence: Identify the first and last presence of a business key,
   and flag whether a business key appears in the latest data uploaded by company info or not.
   rv_xts: The extended tracking satellite table in the raw vault
   pk: The primary key to join hub and xts
   tracked_sat_name: The name of the tracked satellite
#}
{% macro ci_track_business_key_presence(rv_xts, pk, tracked_sat_name) %}
    {% set pk_list = pk if pk is iterable and pk is not string else [pk] %}
    SELECT
        {{ pk_list | join(', ') }},
        MIN(load_datetime) AS first_appear,
        MAX(load_datetime) AS last_appear,
        MAX(load_datetime) = MAX(MAX(load_datetime)) OVER () AS is_in_latest_ci_file
    FROM {{ rv_xts }}
    WHERE satellite_name = {{ "'" ~ tracked_sat_name ~ "'" }}
    GROUP BY {{ pk_list | join(', ') }}
{% endmacro %}
