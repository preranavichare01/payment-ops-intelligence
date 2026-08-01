
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_transactions
from default.mart_ops_dashboard_metrics
where total_transactions is null



  
  
      
    ) dbt_internal_test