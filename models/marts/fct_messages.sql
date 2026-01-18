{{ config(
    materialized='incremental',
    schema='analytics',
    unique_key='message_id',
    tags=['fact', 'messages', 'reporting']
) }}

with staging_messages as (
    select * from {{ ref('stg_telegram_messages') }}
    where message_date is not null
    {% if is_incremental() %}
    -- For incremental loads, only process new messages
    and message_date > (select max(message_date) from {{ this }})
    {% endif %}
),

dim_channels as (
    select channel_key, channel_name from {{ ref('dim_channels') }}
),

dim_dates as (
    select date_key, full_date from {{ ref('dim_dates') }}
),

joined_data as (
    select
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['stg.message_id', 'stg.channel_name']) }} as fact_key,
        
        -- Message identifier
        stg.message_id,
        
        -- Foreign keys
        dc.channel_key,
        dd.date_key,
        
        -- Message content
        stg.message_text,
        stg.message_length,
        stg.message_length_category,
        
        -- Engagement metrics
        stg.views as view_count,
        stg.forwards as forward_count,
        stg.engagement_score,
        
        -- Media information
        stg.has_image_flag as has_image,
        stg.image_path,
        
        -- Channel information
        stg.channel_name,
        stg.channel_title,
        
        -- Time information
        stg.message_date,
        extract(hour from stg.message_date) as hour_of_day,
        
        -- Metadata
        stg.extracted_at,
        current_timestamp as loaded_at
        
    from staging_messages stg
    left join dim_channels dc 
        on stg.channel_name = dc.channel_name
    left join dim_dates dd 
        on date_trunc('day', stg.message_date) = dd.full_date
    
    -- Ensure we have valid dimension keys
    where dc.channel_key is not null
      and dd.date_key is not null
),

final as (
    select
        *,
        -- Calculate performance metrics
        round(view_count * 1.0 / nullif(forward_count, 0), 2) as views_per_forward,
        
        -- Time-based metrics
        case 
            when hour_of_day between 9 and 17 then 'Business Hours'
            when hour_of_day between 18 and 22 then 'Evening'
            when hour_of_day between 23 and 5 then 'Late Night'
            else 'Morning'
        end as posting_time_category,
        
        -- Popularity flags
        case 
            when view_count > 1000 then 'Viral'
            when view_count > 100 then 'Popular'
            else 'Regular'
        end as popularity_level
        
    from joined_data
)

select * from final