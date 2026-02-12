{% macro add_end_datetime_to_sat(metadata_dict) -%}

WITH
records_to_insert AS (
    SELECT *
    FROM (
        {{ automate_dv.sat(**metadata_dict) }}
    )
),
-- noqa: disable=all
{%- if is_incremental() %}

current_active_records AS (
    SELECT *
    FROM {{ this }}
    WHERE
        END_DATETIME IS null
        AND (
            {%- if automate_dv.is_list(metadata_dict.src_pk) %}
            {%- if metadata_dict.src_pk | length == 1 %}
            {{ metadata_dict.src_pk[0] }} IN (SELECT DISTINCT {{ metadata_dict.src_pk[0] }} FROM records_to_insert)
            {%- else %}
            ({{ metadata_dict.src_pk | join(', ') }}) IN (
            SELECT DISTINCT ({{ metadata_dict.src_pk | join(', ') }}) FROM records_to_insert
            )
            {%- endif %}
            {%- else %}
            {{ metadata_dict.src_pk }} IN (SELECT DISTINCT {{ metadata_dict.src_pk }}
            FROM records_to_insert
            )
            {%- endif %}
        )
),
{%- endif %}

full_set AS (
    {%- if is_incremental() %}
    SELECT * EXCEPT (END_DATETIME) FROM current_active_records
    UNION DISTINCT
    {%- endif %}
    SELECT * EXCEPT (END_DATETIME) FROM records_to_insert
)

SELECT
    *,
    LAG(full_set.{{ metadata_dict.src_ldts }})
        OVER (
            {%- if automate_dv.is_list(metadata_dict.src_pk) %}
            {%- if metadata_dict.src_pk | length == 1 %}
            PARTITION BY {{ metadata_dict.src_pk[0] }}
            {%- else %}
            PARTITION BY {{ metadata_dict.src_pk | join(', ') }}
            {%- endif %}
            {%- else %}
            PARTITION BY {{ metadata_dict.src_pk }}
            {%- endif %}
            ORDER BY full_set.{{ metadata_dict.src_ldts }} DESC -- noqa
        ) AS END_DATETIME
FROM full_set

{%- endmacro %}
