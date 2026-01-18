{{ config(
    materialized='table',
    schema='analytics',
    unique_key='detection_id',
    tags=['fact', 'images', 'computer_vision', 'enrichment']
) }}

-- Load YOLO detection results from CSV
with yolo_results as (
    select
        row_number() over (order by processed_at) as detection_id,
        message_id,
        channel_name,
        image_path,
        filename,
        date_folder as extraction_date,
        size_kb,
        
        -- Detection results
        detection_count,
        category,
        confidence_score,
        business_tags,
        top_objects,
        
        -- Parse business insights JSON if exists
        case 
            when business_insights is not null 
            then json_extract_path_text(business_insights::json, 'business_implications', '0') 
            else null 
        end as primary_implication,
        
        -- Extract individual tags for analysis
        case 
            when business_tags like '%promotional%' then true 
            else false 
        end as is_promotional,
        
        case 
            when business_tags like '%product_display%' then true 
            else false 
        end as is_product_display,
        
        case 
            when business_tags like '%lifestyle%' then true 
            else false 
        end as is_lifestyle,
        
        case 
            when business_tags like '%high_confidence%' then true 
            else false 
        end as is_high_confidence,
        
        -- Metadata
        processed_at,
        error
        
    from {{ source('processed', 'yolo_results') }}
    where message_id is not null
      and image_path is not null
),

-- Join with existing messages
messages_with_images as (
    select
        y.detection_id,
        y.message_id,
        y.channel_name,
        y.image_path,
        y.filename,
        y.extraction_date,
        y.size_kb,
        y.detection_count,
        y.category,
        y.confidence_score,
        y.business_tags,
        y.top_objects,
        y.primary_implication,
        y.is_promotional,
        y.is_product_display,
        y.is_lifestyle,
        y.is_high_confidence,
        
        -- Message data
        m.message_text,
        m.message_date,
        m.views,
        m.forwards,
        m.has_image_flag,
        
        -- Channel dimension
        c.channel_key,
        c.channel_type,
        c.total_posts as channel_total_posts,
        c.avg_views as channel_avg_views,
        
        -- Date dimension
        d.date_key,
        d.day_of_week,
        d.is_weekend,
        d.month_name,
        
        -- Engagement metrics
        case 
            when m.views > c.avg_views * 1.5 then 'High Engagement'
            when m.views > c.avg_views then 'Above Average'
            when m.views > 0 then 'Below Average'
            else 'No Engagement'
        end as engagement_level,
        
        -- Performance metrics
        round(m.views::numeric / nullif(c.avg_views, 0), 2) as views_vs_channel_avg,
        
        -- Time analysis
        extract(hour from m.message_date) as post_hour,
        case 
            when extract(hour from m.message_date) between 9 and 17 then 'Business Hours'
            when extract(hour from m.message_date) between 18 and 22 then 'Evening'
            else 'Off Hours'
        end as posting_time,
        
        -- Text analysis
        length(m.message_text) as message_length,
        case 
            when m.message_text ilike '%buy%' or m.message_text ilike '%sale%' then true
            else false
        end as has_sales_keywords,
        
        -- Metadata
        y.processed_at as analysis_timestamp,
        current_timestamp as loaded_at
        
    from yolo_results y
    left join {{ ref('stg_telegram_messages') }} m 
        on y.message_id = m.message_id 
        and y.channel_name = m.channel_name
    left join {{ ref('dim_channels') }} c 
        on y.channel_name = c.channel_name
    left join {{ ref('dim_dates') }} d 
        on date_trunc('day', m.message_date) = d.full_date
    where m.message_id is not null  -- Only include messages we have data for
),

-- Add calculated fields
final as (
    select
        *,
        
        -- Image effectiveness score
        round(
            (confidence_score * 0.3) + 
            (case when detection_count > 0 then 0.2 else 0 end) +
            (case when is_high_confidence then 0.2 else 0 end) +
            (case when views_vs_channel_avg > 1 then 0.3 else 0 end),
            2
        ) as image_effectiveness_score,
        
        -- Category effectiveness
        case 
            when is_promotional and views_vs_channel_avg > 1.2 then 'High Performing Promotional'
            when is_product_display and views_vs_channel_avg > 1.2 then 'High Performing Product'
            when is_lifestyle and views_vs_channel_avg > 1.2 then 'High Performing Lifestyle'
            when views_vs_channel_avg > 1.2 then 'High Performing Other'
            else 'Average/Low Performing'
        end as performance_category,
        
        -- Business recommendation
        case 
            when is_promotional and views_vs_channel_avg > 1.5 then 'Continue promotional style'
            when is_promotional and views_vs_channel_avg < 0.8 then 'Review promotional approach'
            when is_product_display and views_vs_channel_avg > 1.5 then 'Product focus works well'
            when is_lifestyle and views_vs_channel_avg > 1.5 then 'Lifestyle content effective'
            when detection_count = 0 then 'No objects detected - check image quality'
            when confidence_score < 0.4 then 'Low confidence detection'
            else 'Monitor performance'
        end as business_recommendation,
        
        -- Channel-specific insights
        case 
            when channel_type = 'Pharmaceutical' and is_promotional then 'Pharma Promotional'
            when channel_type = 'Pharmaceutical' and is_product_display then 'Pharma Product'
            when channel_type = 'Cosmetics' and is_promotional then 'Cosmetics Promotional'
            when channel_type = 'Cosmetics' and is_lifestyle then 'Cosmetics Lifestyle'
            when channel_type = 'Healthcare' and is_promotional then 'Healthcare Promotional'
            else 'General'
        end as channel_content_type
        
    from messages_with_images
)

select * from final