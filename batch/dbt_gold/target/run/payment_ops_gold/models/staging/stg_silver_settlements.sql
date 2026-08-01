create or replace view default.stg_silver_settlements
  
  
  as
    

select
    settlement_id,
    transaction_id,
    merchant_id,
    settled_amount_inr,
    settlement_status,
    bank_reference_number,
    delay_seconds,
    cast(settlement_timestamp as timestamp) as event_time,
    date(cast(settlement_timestamp as timestamp)) as event_date
from delta.`file:///C:/Users/hp/projects/payment-ops-intelligence/data/processed/silver_settlements`
