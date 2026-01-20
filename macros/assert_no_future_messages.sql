-- Test: Ensure no messages have future dates
-- Returns rows that violate the rule (should be 0)

select 
    message_id,
    channel_name,
    message_date,
    'Future date detected' as test_violation
from {{ ref('stg_telegram_messages') }}
where message_date > current_timestamp + interval '1 day'  -- Allow 1 day grace period for timezone issues