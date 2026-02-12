{% macro ci_sizolockey_postcode(postcode_digit) %}
    {#
        sizolockey consists of SIZOKEY_POSTCODE
        example: D1000001_9602TJ
        the 6-digit postcode = 9602 TJ
        the 4-digit postcode = 9602
    #}
    {% if postcode_digit == 6 %}
        SPLIT(sizolockey, '_')[SAFE_OFFSET(1)]
    {% elif postcode_digit == 4 %}
        SAFE_CAST(LEFT(SPLIT(sizolockey, '_')[SAFE_OFFSET(1)],4) AS INT64)
    {% else %}
        NULL
    {% endif %}
{% endmacro %}
