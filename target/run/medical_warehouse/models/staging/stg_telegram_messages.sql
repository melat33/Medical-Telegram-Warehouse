
  create view "medical_warehouse"."analytics_staging"."stg_telegram_messages__dbt_tmp"
    
    
  as (
    

with source_data as (
    select
        -- Primary key and identifiers
        message_id,
        channel_name,
        channel_title,
        
        -- Date and time (with proper casting and validation)
        case 
            when message_date is not null then message_date::timestamp
            else null
        end as message_date,
        
        -- Text content (clean and truncate if necessary)
        coalesce(nullif(trim(message_text), ''), 'No text content') as message_text,
        
        -- Media information
        has_media,
        case 
            when image_path is not null and image_path != '' then image_path
            else null
        end as image_path,
        
        -- Engagement metrics (with validation)
        greatest(views, 0) as views,  -- Ensure non-negative
        greatest(forwards, 0) as forwards,  -- Ensure non-negative
        
        -- Metadata
        extracted_at,
        
        -- Calculated fields
        length(coalesce(nullif(trim(message_text), ''), '')) as message_length,
        case 
            when image_path is not null and image_path != '' then true
            else false
        end as has_image_flag,
        
        -- Raw data for debugging
        raw_data
        
    from "medical_warehouse"."raw"."telegram_messages"
    
    -- Remove invalid records
    where message_id is not null
      and channel_name is not null
      and trim(channel_name) != ''
),

final as (
    select
        *,
        -- Add additional business logic
        case 
            when message_length > 1000 then 'Long'
            when message_length > 100 then 'Medium'
            when message_length > 0 then 'Short'
            else 'Empty'
        end as message_length_category,
        
        -- Engagement score
        (views * 1.0 + forwards * 5.0) as engagement_score
        
    from source_data
)

select * from final
  );