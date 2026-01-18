select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Test: Ensure message length is reasonable (not extremely long)
-- Returns rows that violate the rule (should be 0)

select 
    message_id,
    channel_name,
    message_length,
    'Extremely long message detected' as test_violation
from "medical_warehouse"."analytics_staging"."stg_telegram_messages"
where message_length > 10000  -- Messages over 10k characters
      
    ) dbt_internal_test