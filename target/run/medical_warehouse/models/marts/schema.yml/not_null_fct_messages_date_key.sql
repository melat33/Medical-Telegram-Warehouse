select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date_key
from "medical_warehouse"."analytics_analytics"."fct_messages"
where date_key is null



      
    ) dbt_internal_test