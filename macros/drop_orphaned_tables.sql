{% macro drop_orphaned_tables(dry_run = true, region = 'europe-west4') %}

    {% if execute %}

        --Setting a list of nodes (models, seeds, snapshots) and schemas that exist in the project
        {% set node_list = graph['nodes'].values() | selectattr("resource_type", "equalto", "model") | list
            + graph['nodes'].values() | selectattr("resource_type", "equalto", "seed")  | list
            + graph['nodes'].values() | selectattr("resource_type", "equalto", "snapshot")  | list %}
        {% set tables_to_keep = [] %}
        {% set schemas_to_check = [] %}

        {%- for node in node_list %}
            {%- if node.alias and node.alias != None %}
                {{ tables_to_keep.append(node.alias) }}
                {% if node.schema not in schemas_to_check %}
                    {{ schemas_to_check.append(node.schema) }}
                {%- endif -%}
            {%- endif -%}
        {%- endfor %}

        --Getting all tables and views that are not in the project anymore
        {% set get_outdated_tables_query %}
                select
                    table_schema,
                    table_name,
                    case
                        when table_type = 'BASE TABLE' then 'table'
                        when table_type = 'VIEW' then 'view'
                    end as table_type,
                    timestamp(creation_time) as creation_time
                from `region-{{ region }}`.INFORMATION_SCHEMA.TABLES
                where
                    table_schema like 'sfg_%'
                    and table_schema in ({{ '\'' + schemas_to_check|join('\', \'') + '\''}})
                    and table_type in ('VIEW', 'BASE TABLE')
                    and table_name not in ({{ '\'' + tables_to_keep|join('\',\'') + '\''}})
        {% endset %}

        {%- set outdated_tables = run_query(get_outdated_tables_query) -%}
        {% do log(outdated_tables|length ~ ' orphaned tables and views found.', info=true)%}

        --Dropping the tables if dry_run = false, otherwise just print the tables that will be deleted
        {%- for outdated_table in outdated_tables %}

            {% set table_to_delete = '`' + target.database + '`.`' + outdated_table['table_schema'] + '`.`' + outdated_table['table_name'] + '`' %}
            {% set drop_query -%}
                drop {{ outdated_table['table_type'] }} if exists {{ table_to_delete }};
            {%- endset %}
            {% do log( drop_query ~' -- creation_time: '~ outdated_table['creation_time'].strftime("%d-%b-%Y %H:%M"), info=true) %}

            {% if dry_run == false %}
                {% do run_query(drop_query) -%}
            {% else %}
                {%- if loop.last %}
                    {% do log('The above objects will be dropped. Please, doublecheck and approve the deletion.', info=true) %}
                {% endif %}
            {% endif %}

        {%- endfor %}

    {% endif %}
{% endmacro %}
