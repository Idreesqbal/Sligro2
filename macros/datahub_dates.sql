{# Data hub events are append-only unlike Fivetran events where the '_Fivetran_deleted' field is updated once a new record comes in
Hence for non-delete events the loaddate should be set to the event time and the enddate should be set to the next event's loaddate
Delete-events are not entered into the Raw Vault as the data doesn't change therefore we do not set load/end-dates  #}
{% macro datahub_loaddate(event_type='event_type') %}

IF({{ event_type }} LIKE '%deleted', null, event_datetime)

{% endmacro %}

{% macro datahub_enddate(partition_columns, event_type='event_type') %}

IF({{ event_type }} LIKE '%deleted', null,
   LEAD(event_datetime) OVER (PARTITION BY {{ partition_columns|join(', ') }} ORDER BY event_datetime ASC))

{% endmacro %}
