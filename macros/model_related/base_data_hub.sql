{% macro base_data_hub(primary_key, payload_columns, source_table, source_extension = '') %}
{% set primary_keys = [primary_key] if primary_key is string else primary_key %}
/* Derived from the ingestion we have multiple rows in the Raw table
   with the same set of attributes, but different event_datetime */
WITH rankedevents AS (
    SELECT
        event_type,
        {{ (primary_keys + payload_columns) | unique | join(', \n') }},

        event_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY
                {{ primary_keys | join(', \n') }},
                event_type
            ORDER BY event_datetime DESC
        ) AS rn
    FROM {{ source("data_hubs_to_dwh_landing_zone", source_table) }} {{ source_extension | trim }}
    WHERE event_datetime IS NOT NULL
)


SELECT
    *,
    -- Adding tracking columns for the satellite
    COALESCE(event_type LIKE '%created%', false) AS is_created,
    COALESCE(event_type LIKE '%deleted', false) AS is_deleted
FROM rankedevents
WHERE rn = 1  -- This will only select the latest event for each unique set of attributes


{% endmacro %}
