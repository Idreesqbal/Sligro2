-- Grant BI view access to any schema that:
-- - starts with dbt_validate
-- - ends with information_marts
{% macro grant_bi_view_permissions(schemas) %}
{% for schema in schemas %}
    {% set sql %}
        GRANT `roles/bigquery.dataViewer`
        ON SCHEMA {{ schema }}
        TO
            "group:gcp-bi-ce@gcp.sligrofoodgroup.cloud",
            "group:gcp-bi-dq@gcp.sligrofoodgroup.cloud",
            "group:gcp-bi-gi@gcp.sligrofoodgroup.cloud",
            "group:gcp-bi-opex@gcp.sligrofoodgroup.cloud";
    {% endset %}
    {% if schema.startswith('dbt_validate') and schema.endswith('information_marts') %}
        {% do run_query(sql) %}
    {% else %}
    {% endif %}
{% endfor %}
{% endmacro %}
