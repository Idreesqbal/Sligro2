{%- set yaml_metadata -%}
source_model:
    sligro60_db260_centraal: rel_voorraad_beheerder_artikel_groep
derived_columns:
    RECORD_SOURCE: "!AS400__rel_voorraad_beheerder_artikel_groep"
    LOAD_DATETIME: "{{ fivetran_loaddate(["ID", "ARTIKEL_GROEP_ID", "LEVERANCIER_ID", "VOORRAAD_BEHEERDER_ID"]) }}"
    END_DATETIME: "{{ fivetran_enddate() }}"
    ARTICLE_GROUP_NK:
        - "TRIM(CAST(ARTIKEL_GROEP_ID AS STRING))"
    SUPPLIER_NK:
        - "TRIM(CAST(LEVERANCIER_ID AS STRING))"
    EMPLOYEE_NK:
        - "TRIM(CAST(VOORRAAD_BEHEERDER_ID AS STRING))"
hashed_columns:
    ARTICLE_GROUP_HK:
        - "ARTICLE_GROUP_NK"
    SUPPLIER_HK:
        - "SUPPLIER_NK"
    EMPLOYEE_HK:
        - "EMPLOYEE_NK"
    ARTICLE_GROUP_SUPPLIER_EMPLOYEE_HK:
        - "ARTICLE_GROUP_NK"
        - "SUPPLIER_NK"
        - "EMPLOYEE_NK"
    HASHDIFF:
        is_hashdiff: true
        columns:
            - "ID"
            - "ARTICLE_GROUP_NK"
            - "SUPPLIER_NK"
            - "EMPLOYEE_NK"
            - "TOEGEVOEGD_DOOR"
            - "TOEGEVOEGD_DATUM"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
      include_source_columns=true,
      source_model=metadata_dict['source_model'],
      derived_columns=metadata_dict['derived_columns'],
      null_columns=none,
      hashed_columns=metadata_dict['hashed_columns'],
      ranked_columns=none
  ) }}
