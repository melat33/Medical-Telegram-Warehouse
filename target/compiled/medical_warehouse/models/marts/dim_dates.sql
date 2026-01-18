

with date_range as (
    select 
        min(date_trunc('day', message_date)) as start_date,
        max(date_trunc('day', message_date)) as end_date
    from "medical_warehouse"."analytics_staging"."stg_telegram_messages"
    where message_date is not null
),

date_series as (
    select 
        generate_series(
            (select start_date from date_range),
            (select end_date from date_range),
            interval '1 day'
        )::date as full_date
),

enriched_dates as (
    select
        full_date,
        -- Date key (YYYYMMDD format)
        to_char(full_date, 'YYYYMMDD')::integer as date_key,
        
        -- Day information
        extract(dow from full_date) + 1 as day_of_week,  -- 1=Sunday, 7=Saturday
        to_char(full_date, 'Day') as day_name,
        extract(day from full_date) as day_of_month,
        
        -- Week information
        extract(week from full_date) as week_of_year,
        to_char(full_date, 'WW') as week_of_month,
        
        -- Month information
        extract(month from full_date) as month_number,
        to_char(full_date, 'Month') as month_name,
        to_char(full_date, 'Mon') as month_short_name,
        
        -- Quarter information
        extract(quarter from full_date) as quarter_number,
        case 
            when extract(quarter from full_date) = 1 then 'Q1'
            when extract(quarter from full_date) = 2 then 'Q2'
            when extract(quarter from full_date) = 3 then 'Q3'
            when extract(quarter from full_date) = 4 then 'Q4'
        end as quarter_name,
        
        -- Year information
        extract(year from full_date) as year_number,
        
        -- Special flags
        case 
            when extract(dow from full_date) in (0, 6) then true  -- 0=Saturday, 6=Sunday
            else false
        end as is_weekend,
        
        case 
            when extract(month from full_date) = 1 and extract(day from full_date) = 1 then true
            else false
        end as is_new_year,
        
        -- Season (Northern Hemisphere)
        case 
            when extract(month from full_date) in (12, 1, 2) then 'Winter'
            when extract(month from full_date) in (3, 4, 5) then 'Spring'
            when extract(month from full_date) in (6, 7, 8) then 'Summer'
            when extract(month from full_date) in (9, 10, 11) then 'Fall'
        end as season
        
    from date_series
),

final as (
    select
        *,
        -- Create a fiscal year (starting April 1st)
        case 
            when extract(month from full_date) >= 4 
            then extract(year from full_date)
            else extract(year from full_date) - 1
        end as fiscal_year,
        
        -- Ethiopian calendar conversion (example)
        extract(year from full_date) - 8 as ethiopian_year,
        
        -- Business day calculation (simple version)
        case 
            when extract(dow from full_date) in (0, 6) then false  -- Weekend
            else true
        end as is_business_day
        
    from enriched_dates
)

select * from final