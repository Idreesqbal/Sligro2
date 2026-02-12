{%- macro scd2_for_subset_of_columns(source_model, src_pk, src_ldts, src_payload) -%}
{#-
  Generates a Type 2 Slowly Changing Dimension (SCD) table based on changes
  to a specified subset of columns (the payload) in a source model.

  It identifies changes by hashing the payload columns and comparing the hash
  with the previous version of the record for the same primary key.
  New versions are created with a 'start_datetime' and an 'end_datetime'
  to define their validity period.

  The macro selects only the primary key columns (src_pk), the payload columns (src_payload),
  and the load timestamp column (src_ldts) from the source, plus derived SCD columns.

  Args:
    source_model (str): The name of the dbt model to use as the source.
                        (e.g., 'stg_my_data')
    src_pk (list or str): A list of column names (or a single column name as a string)
                          that constitute the primary key of the source data.
                          These keys are used to identify unique records over time and
                          will be included in the final output.
                          (e.g., ['customer_id', 'order_id'] or 'user_id')
    src_ldts (str): The name of the column in the source model that represents
                    the load timestamp or the effective date of the record. This column
                    is used to order changes chronologically and will be included in the
                    final output (aliased as start_datetime).
                    (e.g., 'load_timestamp' or 'effective_from')
    src_payload (list or str): A list of column names (or a single column name as a string)
                               from the source model that should be tracked for changes.
                               A new version is created if any of these columns change.
                               These columns will be included in the final output.
                               (e.g., ['address', 'phone_number'] or 'status_column').
                               It is assumed that src_payload inclused the src_pk columns.

  Returns:
    A dataset structured for SCD Type 2, containing:
      - The payload columns specified in `src_payload`.
      - HASHDIFF: The hash of the src_payload columns for the current record.
      - start_datetime: The timestamp when this version of the record became effective
                        (taken directly from the `src_ldts` column).
      - end_datetime: The timestamp when this version of the record ceased to be
                      effective. For the currently active record, this will be the
                      databases default for the LEAD window function when no
                      subsequent row is found (typically NULL).
-#}

WITH
  source_data_with_hash AS (
  SELECT
    {{ src_payload if src_payload is string else src_payload | join(', ') }},
    {{ src_ldts }},
    {{ automate_dv.hash(src_payload, 'HASHDIFF', true) }}
  FROM
    {{ ref(source_model) }}
  ),

  lag_comparison AS (
  SELECT
    *,
    LAG(HASHDIFF) OVER (PARTITION BY {{ src_pk if src_pk is string else src_pk | join(', ') }} ORDER BY {{ src_ldts }} ASC) AS PREV_HASHDIFF
  FROM
    source_data_with_hash
  )

  SELECT
    * EXCEPT (PREV_HASHDIFF),
    {{ src_ldts }} as start_datetime,
    LEAD({{ src_ldts }}) OVER (PARTITION BY {{ src_pk if src_pk is string else src_pk | join(', ') }} ORDER BY {{ src_ldts }} ASC) AS end_datetime
  FROM
    lag_comparison
  WHERE
    HASHDIFF != PREV_HASHDIFF
    OR PREV_HASHDIFF IS NULL

{%- endmacro %}
