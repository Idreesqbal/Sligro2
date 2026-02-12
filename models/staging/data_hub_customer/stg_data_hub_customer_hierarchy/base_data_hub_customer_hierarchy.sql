{{ base_data_hub(
    primary_key = [
        'customerid',
        'hierarchy.hierarchyType',
        'hierarchy.parentCustomerId'
        ],
    payload_columns = [
        'registrationdate',
        'hierarchy.hierarchyType',
        'hierarchy.parentCustomerId'
    ],
    source_table = 'raw_customer_v1',
    source_extension = ', UNNEST(hierarchies) AS hierarchy'
) }}
