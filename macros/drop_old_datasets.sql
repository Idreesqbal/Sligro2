{% macro drop_old_datasets(dry_run = true) %}

    {% set datasets = [
        "sfg_staging"
    ] %}

    {{ log('\nGenerating cleanup queries...\n', info=true) }}

    {% for dataset in datasets %}
        {% set drop_query = 'DROP SCHEMA IF EXISTS `' ~ env_var("DBT_DATA_WAREHOUSE_PROJECT_ID", "not_set") ~ '.' ~ dataset ~ '` CASCADE' %}

        {% if dry_run %}
                {{ log(drop_query, info=true) }}
            {% else %}
            {{ log('Dropping object with command: ' ~ drop_query, info=true) }}
            {% do run_query(drop_query) %}
        {% endif %}
    {% endfor %}
{% endmacro %}
