{{ base_data_hub(
    primary_key = [
        'customerid',
        'characteristic.characteristicType',
        'characteristic.characteristicCode',
        'characteristic.characteristicValue',
        'registrationdate'
        ],
    payload_columns = [
        'registrationdate',
        'characteristic.characteristicType',
        'characteristic.characteristicCode',
        'characteristic.characteristicValue',
    ],
    source_table = 'raw_customer_v1',
    source_extension = ', UNNEST(characteristics) AS characteristic'
) }}
