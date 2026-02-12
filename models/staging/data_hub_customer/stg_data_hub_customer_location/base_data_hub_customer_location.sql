{{ base_data_hub(
    primary_key = [
        'customerid',
        'location.addressid',
        'registrationdate',
        'location.attentionof',
        'location.emailaddress',
        'location.locationname',
        'location.phonenumber'],
    payload_columns = [
        'registrationdate',
        'location.attentionof',
        'location.emailaddress',
        'location.locationname',
        'location.phonenumber',
    ],
    source_table = 'raw_customer_v1',
    source_extension = ', UNNEST(locations) AS location'
) }}
