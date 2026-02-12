{#
Maps a headquarter ID to a country code based on predefined rules.

Example:

Input:
| headquarter_id |
|----------------|
| 5000.0         |
| 5188.0         |
| 5180.0         |
| 1234.0         |

{{ headquarter_to_country(5000.0) }}

Output:
| country_code |
|--------------|
| 'NL'         |
| 'BE'         |
| 'BE'         |
| 'Undefined'  |

Arguments:
- `headquarter_id` (float or integer): The ID representing the headquarter, which will be mapped to a country code.

Returns:
- A string representing the country code based on the `headquarter_id`.
#}

{% macro headquarter_to_country(headquarter_id) %}
   CASE
        WHEN {{ headquarter_id }} = 5000.0 THEN 'NL'
        WHEN {{ headquarter_id }} IN (5188.0, 5180.0) THEN 'BE'
        ELSE 'Undefined'
    END
{% endmacro %}
