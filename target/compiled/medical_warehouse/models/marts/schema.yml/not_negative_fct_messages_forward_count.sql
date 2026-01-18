
    -- Test that ensures column values are not negative
    select *
    from "medical_warehouse"."analytics_analytics"."fct_messages"
    where forward_count < 0
