{{ config(materialized='view') }}

select
    t.transaction_id,
    t.merchant_id,
    t.payment_method,
    t.amount_inr,
    t.status,
    t.event_time as transaction_time,
    s.settlement_id,
    s.settled_amount_inr,
    s.settlement_status,
    s.delay_seconds,
    case when s.delay_seconds > 180 then true else false end as is_delayed,
    t.event_date
from {{ ref('stg_silver_transactions') }} t
left join {{ ref('stg_silver_settlements') }} s
    on t.transaction_id = s.transaction_id
where t.status = 'success'