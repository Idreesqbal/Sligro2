WITH
{#
    Filter out deleted rows by only selecting data
    that are in the latest load cycle of Company Info Postcode4_Afstanden file
#}
link_distance AS (
    {{ ci_filter_deleted_rows(
        rv_hub_link = ref('rv_lnk__postcode_distance'),
        rv_xts = ref('rv_xts__company_reference_postcode4_afstanden'),
        pk = 'postcode_from_to_hk',
        tracked_sat_name = 'rv_sat__postcode_distance'
        ) }}
),

{# sourced from Company Info Postcode4_Afstanden file #}
sat_postcode_distance AS (
    SELECT
        postcode_from_to_hk,
        sizwafst
    FROM {{ ref('rv_sat__postcode_distance') }}
    WHERE end_datetime IS null
),

result AS (
    SELECT
        CONCAT(hub_postcode_from.postcode_bk, '||', hub_postcode_to.postcode_bk) AS pk_postal_code_distance,
        hub_postcode_from.postcode_bk AS postal_code_from_4_digit,
        hub_postcode_to.postcode_bk AS postal_code_to_4_digit,
        CAST(REPLACE(sat_postcode_distance.sizwafst, ',', '.') AS DECIMAL) AS distance_in_km
    FROM link_distance
    INNER JOIN {{ ref('rv_hub__postcode') }} AS hub_postcode_from
        ON link_distance.postcode_from_hk = hub_postcode_from.postcode_hk
    INNER JOIN {{ ref('rv_hub__postcode') }} AS hub_postcode_to
        ON link_distance.postcode_to_hk = hub_postcode_to.postcode_hk
    INNER JOIN sat_postcode_distance
        ON link_distance.postcode_from_to_hk = sat_postcode_distance.postcode_from_to_hk
)

SELECT *
FROM result
