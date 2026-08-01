
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_method
from default.mart_ops_dashboard_metrics
where payment_method is null



  
  
      
    ) dbt_internal_test