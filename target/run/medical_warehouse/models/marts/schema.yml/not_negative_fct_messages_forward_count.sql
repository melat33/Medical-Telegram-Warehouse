select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    -- Test that ensures column values are not negative
    select *
    from "medical_warehouse"."analytics_analytics"."fct_messages"
    where forward_count < 0

      
    ) dbt_internal_test