{{ base_data_hub(
    primary_key = [
        'customerid',
        'customer_group.groupId',
        'customer_group.description',
        'customer_group.typeCode',
        'customer_group.typeDescription'
        ],
    payload_columns = [
        'registrationdate',
        'customer_group.description',
        'customer_group.groupId',
        'customer_group.typeCode',
        'customer_group.typeDescription'
    ],
    source_table = 'raw_customer_v1',
    source_extension = ', UNNEST(`groups`) AS customer_group'
) }}
