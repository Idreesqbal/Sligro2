{% macro drop_empty_dev_datasets(dry_run = true) %}

    {% if execute %}
    -- Getting the list of schemas that start with sfg_ and dbt_
        {% set get_dataset_list %}
            select schema_name from `{{ env_var("DBT_DATA_WAREHOUSE_PROJECT_ID", "not_set") }}`.INFORMATION_SCHEMA.SCHEMATA
            where schema_name like 'dbt_%'
        {% endset %}
        {% set datasets = run_query(get_dataset_list).columns[0].values() %}
        {% if datasets|length <= 0 %}
            {% do exceptions.raise_compiler_error('no datasets found!') %}
        {% endif %}

        {%- for dataset in datasets %}
            {% set get_tables %}
                select table_name from {{ dataset }}.INFORMATION_SCHEMA.TABLES
            {% endset %}
            {% set tables = run_query(get_tables).columns[0].values() %}

            {% if tables|length == 0 %}

                {% set fqn = '`' + target.database + '`.`' + dataset + '`' %}
                {% if dry_run == false %}
                    {% call statement() -%}
                        {% do log('dropping dataset: ' ~ fqn, info=true) %}
                        drop schema if exists {{ fqn }};
                    {%- endcall %}
                {% elif dry_run == true %}
                    {% do log( 'drop schema if exists ' ~ fqn ~';', info=true) %}
                    {%- if loop.last %}
                        {% do log('The above datasets will be dropped. Please, doublecheck and approve the deletion.\n', info=true) %}
                    {% endif %}
                {% endif %}

            {% endif %}
        {%- endfor %}
    {% endif %}
{% endmacro %}
