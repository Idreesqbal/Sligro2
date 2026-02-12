WITH

supplier AS (
    SELECT
        hub_supplier.supplier_bk,
        sat_supplier.vertc1,
        sat_suplier_contact.accmc1,
        sat_supplier.emalc1,
        sat_suplier_contact.emalc2,
        sat_suplier_contact.emalc3,
        sat_suplier_contact.emalc4,
        sat_suplier_contact.emalc5,
        sat_suplier_contact.cntpc1,
        sat_suplier_contact.cntpc2,
        sat_suplier_contact.cntpc3,
        sat_suplier_contact.fomsc1,
        sat_suplier_contact.fomsc2,
        sat_suplier_contact.fomsc3,
        sat_supplier.tlnrc1,
        sat_suplier_contact.tlnrc2,
        sat_suplier_contact.tlnrc3,
        sat_suplier_contact.tlnrc4,
        sat_suplier_contact.tlnrc5
    FROM {{ ref("rv_hub__supplier") }} AS hub_supplier
    INNER JOIN {{ ref("bv_sat__supplier_general_info") }} AS sat_supplier
        ON hub_supplier.supplier_hk = sat_supplier.supplier_hk
    LEFT JOIN {{ ref("rv_sat__supplier_contacts") }} AS sat_suplier_contact
        ON hub_supplier.supplier_hk = sat_suplier_contact.supplier_hk
    WHERE
        sat_supplier.end_datetime IS null
        AND sat_suplier_contact.end_datetime IS null
),
{# Because every contact should be a row on its own
and in the source all contacts are separeated over different columns for one supplier,
I created a dataset per contact type. #}
representer AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        'VERTEGENWOORDIGER' AS contact_type,
        vertc1 AS contact_name,
        tlnrc1 AS phone_number,
        emalc1 AS mail,
        {# a supplier can have multiple contacts, only the supplier_bk is not sufficient.
        Therefore the addition of the contact_type #}
        CONCAT(supplier_bk, '||', 'VERTEGENWOORDIGER') AS pk_supplier_contact_code
    FROM supplier
    WHERE
        COALESCE(vertc1, '') != ''
        AND COALESCE(tlnrc1, '') != ''
        AND COALESCE(emalc1, '') != ''
),

account_manager AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        'ACCOUNTMANAGER' AS contact_type,
        accmc1 AS contact_name,
        tlnrc5 AS phone_number,
        emalc5 AS mail,
        CONCAT(supplier_bk, '||', 'ACCOUNTMANAGER') AS pk_supplier_contact_code
    FROM supplier
    WHERE
        COALESCE(accmc1, '') != ''
        AND COALESCE(tlnrc5, '') != ''
        AND COALESCE(emalc5, '') != ''
),

order_contact AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        fomsc1 AS contact_type,
        cntpc1 AS contact_name,
        tlnrc2 AS phone_number,
        emalc2 AS mail,
        CONCAT(supplier_bk, '||', fomsc1) AS pk_supplier_contact_code
    FROM supplier
    WHERE
        COALESCE(fomsc1, '') != ''
        AND COALESCE(cntpc1, '') != ''
        AND COALESCE(tlnrc2, '') != ''
        AND COALESCE(emalc2, '') != ''
),

invoice_contact AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        fomsc2 AS contact_type,
        cntpc2 AS contact_name,
        tlnrc3 AS phone_number,
        emalc3 AS mail,
        CONCAT(supplier_bk, '||', fomsc2) AS pk_supplier_contact_code
    FROM supplier
    WHERE
        COALESCE(fomsc2, '') != ''
        AND COALESCE(cntpc2, '') != ''
        AND COALESCE(tlnrc3, '') != ''
        AND COALESCE(emalc3, '') != ''
),

quality_contact AS (
    SELECT DISTINCT
        supplier_bk AS fk_supplier_code,
        fomsc3 AS contact_type,
        cntpc3 AS contact_name,
        tlnrc4 AS phone_number,
        emalc4 AS mail,
        CONCAT(supplier_bk, '||', fomsc3) AS pk_supplier_contact_code
    FROM supplier
    WHERE
        COALESCE(fomsc3, '') != ''
        AND COALESCE(cntpc3, '') != ''
        AND COALESCE(tlnrc4, '') != ''
        AND COALESCE(emalc4, '') != ''
),
{# To adjust the order of the columns that is accepted by the linting
and to minimalise/dynamicaly union all the tables #}
unioning AS (
    {% set list_of_columns = [
        'pk_supplier_contact_code',
        'fk_supplier_code',
        'contact_type',
        'contact_name',
        'phone_number',
        'mail'
    ] -%}
    {% set list_of_ctes = ['representer', 'account_manager', 'order_contact', 'invoice_contact', 'quality_contact'] -%}
    {% for cte in list_of_ctes %}
        SELECT {{ list_of_columns | join(', ') }}
        FROM {{ cte }}
        {% if not loop.last %}
            UNION DISTINCT
        {% endif %}
    {%- endfor %}
)

SELECT
    pk_supplier_contact_code AS pk_supplier_contact_person_code,
    fk_supplier_code,
    contact_type,
    contact_name,
    phone_number,
    mail AS email_address
FROM unioning
