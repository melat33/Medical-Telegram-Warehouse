select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fact_key
from "medical_warehouse"."analytics_analytics"."fct_messages"
where fact_key is null



      
    ) dbt_internal_test