
    
    

select
    fact_key as unique_field,
    count(*) as n_records

from "medical_warehouse"."analytics_analytics"."fct_messages"
where fact_key is not null
group by fact_key
having count(*) > 1


