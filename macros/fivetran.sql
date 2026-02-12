{% macro fivetran_enddate() %}

IF(_fivetran_deleted = true, _fivetran_synced, null)

{% endmacro %}

{% macro fivetran_index(keys=['_fivetran_id']) %}

ROW_NUMBER() OVER (
           PARTITION BY {{ keys|join(', ') }}
           ORDER BY _fivetran_deleted DESC, _fivetran_synced
        )

{% endmacro %}

{% macro fivetran_loaddate(partition_columns) %}
        IF(_fivetran_deleted = true,
                COALESCE(
                        LAG(_fivetran_synced) OVER (
                                PARTITION BY {{ '_fivetran_deleted, ' ~ partition_columns|join(', ') }}
                                ORDER BY _fivetran_synced ASC),
                        TIMESTAMP_SUB(_fivetran_synced, INTERVAL 1 MICROSECOND)
                        ),
                _fivetran_synced
        )
{% endmacro %}

{% macro fivetran_last_touched() %}
    IF(_fivetran_active = true,
        _fivetran_start,
        _fivetran_end
    )
{% endmacro %}


{% macro fivetran_end() %}
  {% set relation = this %}
  {% set columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}

  {% if '_fivetran_end' in columns %}
    IF(_fivetran_end < '9999-12-31', _fivetran_end, NULL)
  {% else %}
    NULL
  {% endif %}
{% endmacro %}
