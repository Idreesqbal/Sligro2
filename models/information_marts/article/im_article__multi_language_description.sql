WITH

hub_article AS (
    SELECT
        article_hk,
        article_bk
    FROM {{ ref("rv_hub__article") }}
),

{# Create a table of all article locale combinations to facilitate joins in the result table #}
article_locale_combinations AS (
    {%- set list_of_satellites = [
        'rv_masat__article_allergen_specification',
        'rv_masat__article_description',
        'rv_masat__article_instructions',
        'rv_masat__article_operational_information'
    ] -%}
    {%- if list_of_satellites | length > 0 %}
        SELECT DISTINCT
            article_hk,
            locale
        FROM
            {{ ref(list_of_satellites[0]) }}
        WHERE
            is_deleted = false
            AND locale IS NOT null
        {%- for satellite in list_of_satellites[1:] %}
            UNION DISTINCT
            SELECT
                article_hk,
                locale
            FROM
                {{ ref(satellite) }}
            WHERE
                is_deleted = false
                AND locale IS NOT null
        {%- endfor -%}
    {%- else %}
        SELECT NULL AS article_hk, NULL AS locale
    {%- endif %}
),

latest_allergen_active AS (
    SELECT
        article_hk,
        locale,
        MAX(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_masat__article_allergen_specification") }}
    WHERE is_deleted = false
    GROUP BY article_hk, locale
),

latest_allergen AS (
    SELECT
        allergen.article_hk,
        allergen.locale,
        allergen.allergenstatement AS allergen_statement
    FROM {{ ref("rv_masat__article_allergen_specification") }} AS allergen
    INNER JOIN latest_allergen_active AS latest
        ON
            allergen.article_hk = latest.article_hk
            AND allergen.locale = latest.locale
            AND allergen.load_datetime = latest.max_load_datetime
    WHERE allergen.is_deleted = false
),

latest_description AS (
    WITH latest_descriptions AS (
        SELECT
            description.article_hk,
            description.locale,
            description.traceabilitytype AS traceability_type,
            description.scientificname AS scientific_name,
            description.scalesnutrientsdeclaration AS scales_nutrients_declaration,
            description.scalesproductname AS scales_product_name,
            description.scalesgroup AS scales_group,
            description.materialtype AS material_type,
            description.scalesingredientsdeclaration AS scales_ingredients_declaration,
            description.countryoflastprocessing AS country_of_last_processing,
            description.additionallabelinformation AS additional_label,
            description.legalname AS legal_name,
            description.mainstatus AS main_status,
            description.countryoforigin AS country_of_origin,
            description.articledescription AS article_description,
            ROW_NUMBER()
                OVER (PARTITION BY description.article_hk, description.locale ORDER BY description.load_datetime DESC)
                AS row_num
        FROM {{ ref("rv_masat__article_description") }} AS description
        WHERE description.is_deleted = false
    )

    SELECT
        article_hk,
        locale,
        traceability_type,
        scientific_name,
        scales_nutrients_declaration,
        scales_product_name,
        scales_group,
        material_type,
        scales_ingredients_declaration,
        country_of_last_processing,
        additional_label,
        legal_name,
        main_status,
        country_of_origin,
        article_description
    FROM latest_descriptions
    WHERE row_num = 1
),

latest_instructions_active AS (
    SELECT
        article_hk,
        locale,
        MAX(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_masat__article_instructions") }}
    WHERE is_deleted = false
    GROUP BY article_hk, locale

),

latest_instructions AS (
    SELECT
        instructions.article_hk,
        instructions.locale,
        instructions.preparationinstructions AS preparation_instructions,
        instructions.defrostinstructions AS defrost_instructions,
        instructions.consumerusageinstructions AS consumer_usage_instructions,
        instructions.consumerstorageinstructions AS consumer_storage_instructions
    FROM {{ ref("rv_masat__article_instructions") }} AS instructions
    INNER JOIN latest_instructions_active AS latest
        ON
            instructions.article_hk = latest.article_hk
            AND instructions.locale = latest.locale
            AND instructions.load_datetime = latest.max_load_datetime
    WHERE instructions.is_deleted = false
),

latest_operational_active AS (
    SELECT
        article_hk,
        locale,
        MAX(load_datetime) AS max_load_datetime
    FROM {{ ref("rv_masat__article_operational_information") }}
    WHERE is_deleted = false
    GROUP BY article_hk, locale
),

latest_operational AS (
    SELECT
        operational.article_hk,
        operational.locale,
        operational.scalesminimumshelflife AS scales_minimum_shelf_life,
        operational.scalesminimumshelflifeuom AS scales_minimum_shelf_life_uom,
        operational.minimumshelflifeafterproduction AS minimum_shelf_life_after_production,
        operational.minimumshelflifeafterproductionuom AS minimum_shelf_life_after_production_uom,
        operational.maximumstoragetemperature AS maximum_storage_temperature,
        operational.maximumstoragetemperatureuom AS maximum_storage_temperature_uom
    FROM {{ ref("rv_masat__article_operational_information") }} AS operational
    INNER JOIN latest_operational_active AS latest
        ON
            operational.article_hk = latest.article_hk
            AND operational.locale = latest.locale
            AND operational.load_datetime = latest.max_load_datetime
    WHERE operational.is_deleted = false
),

result AS (
    SELECT
        hub.article_bk AS fk_article_code,
        article_locale.locale AS fk_locale_code,
        masat_allergen.allergen_statement,
        masat_description.traceability_type,
        masat_description.scientific_name,
        masat_description.scales_nutrients_declaration,
        masat_description.scales_product_name,
        masat_description.scales_group,
        masat_description.material_type,
        masat_description.scales_ingredients_declaration,
        masat_description.country_of_last_processing,
        masat_description.additional_label,
        masat_description.legal_name,
        masat_description.main_status,
        masat_description.country_of_origin,
        masat_description.article_description,
        masat_instructions.preparation_instructions,
        masat_instructions.defrost_instructions,
        masat_instructions.consumer_usage_instructions,
        masat_instructions.consumer_storage_instructions,
        masat_operational.scales_minimum_shelf_life,
        masat_operational.scales_minimum_shelf_life_uom,
        masat_operational.minimum_shelf_life_after_production,
        masat_operational.minimum_shelf_life_after_production_uom,
        masat_operational.maximum_storage_temperature,
        masat_operational.maximum_storage_temperature_uom,
        CONCAT(hub.article_bk, "||", article_locale.locale) AS pk_multi_language_description_code
    FROM
        hub_article AS hub
        {# Expand the hub to generate rows for every locale that an article has data for #}
    LEFT JOIN article_locale_combinations AS article_locale ON hub.article_hk = article_locale.article_hk
    LEFT JOIN
        latest_allergen AS masat_allergen
        ON hub.article_hk = masat_allergen.article_hk AND article_locale.locale = masat_allergen.locale
    LEFT JOIN
        latest_description AS masat_description
        ON hub.article_hk = masat_description.article_hk AND article_locale.locale = masat_description.locale
    LEFT JOIN
        latest_instructions AS masat_instructions
        ON hub.article_hk = masat_instructions.article_hk AND article_locale.locale = masat_instructions.locale
    LEFT JOIN
        latest_operational AS masat_operational
        ON hub.article_hk = masat_operational.article_hk AND article_locale.locale = masat_operational.locale
    WHERE
        article_locale.locale IS NOT null
)

SELECT
    pk_multi_language_description_code,
    fk_article_code,
    fk_locale_code,
    allergen_statement,
    traceability_type,
    scientific_name,
    scales_nutrients_declaration,
    scales_product_name,
    scales_group,
    material_type,
    scales_ingredients_declaration,
    country_of_last_processing,
    additional_label,
    legal_name,
    main_status,
    country_of_origin,
    article_description,
    preparation_instructions,
    defrost_instructions,
    consumer_usage_instructions,
    consumer_storage_instructions,
    scales_minimum_shelf_life,
    scales_minimum_shelf_life_uom,
    minimum_shelf_life_after_production,
    minimum_shelf_life_after_production_uom,
    maximum_storage_temperature,
    maximum_storage_temperature_uom
FROM result
