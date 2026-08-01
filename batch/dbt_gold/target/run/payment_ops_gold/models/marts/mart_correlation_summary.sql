
  
    
        create or replace table default.mart_correlation_summary
      
      
    using delta
      
      
      
      
      
      

      as
      

select
    classification,
    affected_banks,
    detected_at
from delta.`file:///C:/Users/hp/projects/payment-ops-intelligence/data/processed/gold_correlation_events`
  