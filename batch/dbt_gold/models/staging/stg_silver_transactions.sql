{{ config(materialized='view') }}

with parsed as (
    select
        from_json(
            value,
            'transaction_id STRING, user_id STRING, merchant_id STRING, merchant_category STRING, payment_method STRING, amount_inr DOUBLE, currency STRING, status STRING, bank_response_code STRING, response_time_ms INT, is_international BOOLEAN, device_type STRING, city STRING, timestamp STRING'
        ) as data
    from delta.`{{ var('project_root') }}/data/processed/bronze_transactions`
)
select
    data.transaction_id,
    data.user_id,
    data.merchant_id,
    data.merchant_category,
    data.payment_method,
    data.amount_inr,
    data.status,
    data.bank_response_code,
    data.response_time_ms,
    data.city,
    cast(data.timestamp as timestamp) as event_time,
    date(cast(data.timestamp as timestamp)) as event_date
from parsed
where data.transaction_id is not null