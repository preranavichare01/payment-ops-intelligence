{{ config(materialized='table', file_format='delta') }}

select
    event_date,
    payment_method,
    merchant_category,
    count(*) as total_transactions,
    sum(case when status = 'success' then 1 else 0 end) as success_count,
    sum(case when status = 'timeout' then 1 else 0 end) as timeout_count,
    sum(amount_inr) as total_volume_inr,
    avg(response_time_ms) as avg_response_time_ms
from {{ ref('stg_silver_transactions') }}
group by event_date, payment_method, merchant_category