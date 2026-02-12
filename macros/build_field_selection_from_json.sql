{%- macro build_field_selection_from_json(fields, json_path='$', parent_alias='', source_column='') -%}
{%- for field in fields -%}
  {%- set field_name = field.name -%}
  {%- set field_type = field.type -%}
  {%- set field_mode = field.get('mode', 'NULLABLE') -%}
  {%- set current_path = json_path + '.' + field_name -%}

  {%- if field_type == 'RECORD' -%}
    {%- if field_mode == 'REPEATED' -%}
      {%- if field.fields and field.fields|length > 0 -%}
    -- Handle repeated RECORD (array of structs)
    ARRAY(
      SELECT AS STRUCT
        {{ build_field_selection_from_json(field.fields, '$', 'nested_' + field_name + '_item', 'nested_' + field_name + '_item') | indent(4) }}
      FROM UNNEST(JSON_QUERY_ARRAY({{ parent_alias or source_column }}, '{{ current_path }}')) as nested_{{ field_name }}_item
    ) as {{ field_name }}
      {%- else -%}
    -- Handle empty repeated RECORD (array of empty structs)
    JSON_QUERY_ARRAY({{ parent_alias or source_column }}, '{{ current_path }}') as {{ field_name }}
      {%- endif -%}
    {%- else -%}
    -- Handle nested RECORD (single struct)
    STRUCT(
      {{ build_field_selection_from_json(field.fields, current_path, parent_alias, source_column) | indent(2) }}
    ) as {{ field_name }}
    {%- endif -%}
  {%- else -%}
    {%- if field_mode == 'REPEATED' -%}
      {%- if field_type == 'STRING' -%}
    -- Handle repeated primitive field
    JSON_QUERY_ARRAY({{ parent_alias or source_column }}, '{{ current_path }}') as {{ field_name }}
      {%- elif field_type == 'INTEGER' or field_type == 'INT64' -%}
    -- Handle repeated primitive field
    ARRAY(
      SELECT CAST(JSON_VALUE(item, '$') AS INT64)
      FROM UNNEST(JSON_QUERY_ARRAY({{ parent_alias or source_column }}, '{{ current_path }}')) as item
    ) as {{ field_name }}
      {%- elif field_type == 'FLOAT' or field_type == 'FLOAT64' -%}
    -- Handle repeated primitive field
    ARRAY(
      SELECT CAST(JSON_VALUE(item, '$') AS FLOAT64)
      FROM UNNEST(JSON_QUERY_ARRAY({{ parent_alias or source_column }}, '{{ current_path }}')) as item
    ) as {{ field_name }}
      {%- elif field_type == 'BOOLEAN' -%}
    -- Handle repeated primitive field
    ARRAY(
      SELECT CAST(JSON_VALUE(item, '$') AS BOOLEAN)
      FROM UNNEST(JSON_QUERY_ARRAY({{ parent_alias or source_column }}, '{{ current_path }}')) as item
    ) as {{ field_name }}
      {%- else -%}
    -- Handle repeated primitive field
    JSON_QUERY_ARRAY({{ parent_alias or source_column }}, '{{ current_path }}') as {{ field_name }}
      {%- endif -%}
    {%- else -%}
      {%- if field_type == 'STRING' -%}
    -- Handle single primitive field
    JSON_VALUE({{ parent_alias or source_column }}, '{{ current_path }}') as {{ field_name }}
      {%- elif field_type == 'INTEGER' or field_type == 'INT64' -%}
    -- Handle single primitive field
    CAST(JSON_VALUE({{ parent_alias or source_column }}, '{{ current_path }}') AS INT64) as {{ field_name }}
      {%- elif field_type == 'FLOAT' or field_type == 'FLOAT64' -%}
    -- Handle single primitive field
    CAST(JSON_VALUE({{ parent_alias or source_column }}, '{{ current_path }}') AS FLOAT64) as {{ field_name }}
      {%- elif field_type == 'BOOLEAN' -%}
    -- Handle single primitive field
    CAST(JSON_VALUE({{ parent_alias or source_column }}, '{{ current_path }}') AS BOOLEAN) as {{ field_name }}
      {%- elif field_type == 'TIMESTAMP' -%}
    -- Handle single primitive field
    CAST(JSON_VALUE({{ parent_alias or source_column }}, '{{ current_path }}') AS TIMESTAMP) as {{ field_name }}
      {%- elif field_type == 'DATE' -%}
    -- Handle single primitive field
    CAST(JSON_VALUE({{ parent_alias or source_column }}, '{{ current_path }}') AS DATE) as {{ field_name }}
      {%- else -%}
    -- Handle single primitive field
    JSON_VALUE({{ parent_alias or source_column }}, '{{ current_path }}') as {{ field_name }}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  {%- if not loop.last -%},

  {%- endif -%}
{%- endfor -%}
{%- endmacro -%}
