create or replace view default.int_bank_health
  
  
  as
    

select
    payment_method as bank,
    date_trunc('hour', event_time) as hour_bucket,
    count(*) as total_transactions,
    sum(case when status = 'success' then 1 else 0 end) as success_count,
    (sum(case when status = 'success' then 1 else 0 end) / count(*)) * 100 as success_rate_pct,
    avg(response_time_ms) as avg_response_time_ms,
    percentile_approx(response_time_ms, 0.5) as p50_response_time_ms,
    percentile_approx(response_time_ms, 0.95) as p95_response_time_ms
from default.stg_silver_transactions
group by payment_method, date_trunc('hour', event_time)
