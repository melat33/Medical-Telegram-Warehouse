
    
    

with child as (
    select channel_key as from_field
    from "medical_warehouse"."analytics_analytics"."fct_messages"
    where channel_key is not null
),

parent as (
    select channel_key as to_field
    from "medical_warehouse"."analytics_analytics"."dim_channels"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


