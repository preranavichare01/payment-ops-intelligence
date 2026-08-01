

select
    event_date,
    merchant_id,
    count(*) as total_settled_transactions,
    sum(case when is_delayed then 1 else 0 end) as delayed_count,
    sum(amount_inr) as total_txn_amount,
    sum(settled_amount_inr) as total_settled_amount,
    sum(amount_inr) - sum(settled_amount_inr) as amount_discrepancy
from default.int_payment_enriched
group by event_date, merchant_id