-- Test: Ensure view counts are non-negative
-- Returns rows that violate the rule (should be 0)

select 
    message_id,
    channel_name,
    views,
    'Negative views detected' as test_violation
from "medical_warehouse"."analytics_staging"."stg_telegram_messages"
where views < 0