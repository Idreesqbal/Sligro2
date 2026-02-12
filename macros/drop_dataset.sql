{% macro drop_dataset(dataset) %}

    {% set drop_query = 'DROP SCHEMA IF EXISTS `' ~ env_var("DBT_DATA_WAREHOUSE_PROJECT_ID", "not_set") ~ '.' ~ dataset ~ '` CASCADE' %}
    {{ log('Dropping object with command: ' ~ drop_query, info=true) }}
    {% do run_query(drop_query) %}

{% endmacro %}
