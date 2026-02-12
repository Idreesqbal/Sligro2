{% macro ci_select_ref_labels(columns) %}
{# ci_select_ref_labels: select the English description if available; otherwise, use the Dutch description.
 Example: ['SIZOSBIL', 'SIZOSBI2L']
 result:
 COALESCE(SIZOSBIL_label.siztlabeng, SIZOSBIL_label.siztlab) AS sizosbil_desc,
 COALESCE(SIZOSBI2L_label.siztlabeng, SIZOSBI2L_label.siztlab) AS sizosbi2l_desc,
#}
    {%- for column in columns -%}
        COALESCE({{ column }}_label.siztlabeng, {{ column }}_label.siztlab) AS {{ column | lower }}_desc{% if not loop.last %},{% endif %}
    {%- endfor -%}
{% endmacro %}


{%- macro ci_lookup_ref_labels(identifier, columns) -%}
{# ci_lookup_ref_labels: turn the list into joins.
 Example: identifier=BAG_Pand, columns=['SIZOSBIL', 'SIZOSBI2L']
 result:
 LEFT JOIN ref__company_reference_labels AS SIZOSBIL_label
 ON SIZOSBIL_label.company_reference_labels_bk = CONCAT('BAG_Pand||SIZOSBIL||', sizosbil )
 LEFT JOIN ref__company_reference_labels AS SIZOSBI2L_label
 ON SIZOSBI2L_label.company_reference_labels_bk = CONCAT('BAG_Pand||SIZOSBI2L||', sizosbi2l )
#}
    {%- for column in columns -%}
        LEFT JOIN {{ ref('ref__company_reference_labels') }} AS {{ column }}_label
        ON {{ column }}_label.company_reference_labels_bk = CONCAT('{{ identifier }}||{{ column }}||', {{ column | lower }} )
    {%- endfor -%}
{%- endmacro -%}
