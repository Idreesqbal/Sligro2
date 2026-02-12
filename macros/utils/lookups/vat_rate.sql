{% macro vat_rate(vat_code) %}
    CASE {{ vat_code }}
        WHEN 'A' THEN 0.21
        WHEN 'B' THEN 0.09
        WHEN 'C' THEN 0
        WHEN 'D' THEN 0.12
    END
{% endmacro %}
