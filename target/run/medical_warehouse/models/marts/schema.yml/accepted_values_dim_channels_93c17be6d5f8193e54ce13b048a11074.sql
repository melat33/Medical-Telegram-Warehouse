select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        channel_type as value_field,
        count(*) as n_records

    from "medical_warehouse"."analytics_analytics"."dim_channels"
    group by channel_type

)

select *
from all_values
where value_field not in (
    'Pharmaceutical','Cosmetics','Healthcare','General Medical'
)



      
    ) dbt_internal_test