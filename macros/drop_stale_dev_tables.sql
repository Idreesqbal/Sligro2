{% macro drop_stale_dev_tables(stale_days = 30, dry_run = true) %}

    {% if execute %}

        {%- if stale_days < 15 %}
            {% do exceptions.raise_compiler_error('Stale_days must be 15 or greater!') %}
        {%- endif -%}

        -- Getting the list of schemas that start with sfg_ and dbt_
        {% set get_schema_list %}
            select schema_name from `{{ env_var("DBT_DATA_WAREHOUSE_PROJECT_ID", "not_set") }}`.INFORMATION_SCHEMA.SCHEMATA
            where schema_name like 'dbt_%'
        {% endset %}
        {% set schema_names = run_query(get_schema_list).columns[0].values() %}
        {% if schema_names|length <= 0 %}
            {% do exceptions.raise_compiler_error('No schemas found!') %}
        {% endif %}

        --Getting all tables and views that are not in the project, or stale
        {% call statement('get_outdated_tables', fetch_result=True) %}
            with
            tables_and_views as (
                select
                    table_schema as schema_name,
                    table_name as ref_name,
                    case
                        when table_type = 'VIEW' then 'view'
                        else 'table'
                    end as ref_type,
                    TIMESTAMP(creation_time) as last_altered
                from (
                    {%- for schema in schema_names %}
                        select * from {{ schema }}.INFORMATION_SCHEMA.TABLES
                        {% if not loop.last %}union all {% endif %}
                    {%- endfor -%}
                )
            )

            select
                schema_name,
                ref_name,
                ref_type,
                last_altered
            from tables_and_views
            where last_altered < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ stale_days }} DAY)

        {% endcall %}

        {%- set get_outdated_tables = load_result('get_outdated_tables') -%}

        {% do log(get_outdated_tables['data']|length ~ ' stale tables and views found.', info=true)%}

        --Dropping the tables if dry_run = false, otherwise print the tables that will be deleted
        {%- for to_delete in get_outdated_tables['data'] %}
            {% set fqn = '`' + target.database + '`.`' + to_delete[0] + '`.`' + to_delete[1] + '`' %}
            {% if dry_run == false %}
                {% call statement() -%}
                    {% do log('Dropping ' ~ to_delete[2] ~ ': ' ~ fqn, info=true) %}
                    drop {{ to_delete[2] }} if exists {{ fqn }};
                {%- endcall %}
            {% elif dry_run == true %}
                {% do log( 'drop '~to_delete[2] ~ ' if exists ' ~ fqn ~'; -- last_altered: '~ to_delete[3].strftime("%d-%b-%Y %H:%M") ~'; row_count: '~ to_delete[4], info=true) %}
                {%- if loop.last %}
                    {% do log('The above objects will be dropped. Please, doublecheck and approve the deletion.\n', info=true) %}
                {% endif %}
            {% endif %}
        {%- endfor %}
        {%- set response = get_outdated_tables['response'] %}
        {% do log('Query Status + # of results found: '~ response, info=true) %}
    {% endif %}
{% endmacro %}
