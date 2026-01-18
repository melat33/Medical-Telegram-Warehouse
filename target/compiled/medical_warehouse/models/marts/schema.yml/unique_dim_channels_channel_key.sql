
    
    

select
    channel_key as unique_field,
    count(*) as n_records

from "medical_warehouse"."analytics_analytics"."dim_channels"
where channel_key is not null
group by channel_key
having count(*) > 1


