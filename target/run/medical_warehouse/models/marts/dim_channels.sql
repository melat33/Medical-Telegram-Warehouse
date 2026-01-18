
  
    

  create  table "medical_warehouse"."analytics_analytics"."dim_channels__dbt_tmp"
  
  
    as
  
  (
    

with channel_data as (
    select
        channel_name,
        channel_title,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(views) as avg_views,
        avg(forwards) as avg_forwards,
        sum(case when has_image_flag then 1 else 0 end) as posts_with_images,
        avg(engagement_score) as avg_engagement_score
    from "medical_warehouse"."analytics_staging"."stg_telegram_messages"
    where channel_name is not null
    group by 1, 2
),

channel_categories as (
    select
        *,
        case 
            when lower(channel_name) like '%chemed%' or 
                 lower(channel_name) like '%med%' or
                 lower(channel_name) like '%pharm%' then 'Pharmaceutical'
            when lower(channel_name) like '%cosmetic%' or
                 lower(channel_name) like '%lobelia%' then 'Cosmetics'
            when lower(channel_name) like '%health%' or
                 lower(channel_name) like '%care%' then 'Healthcare'
            else 'General Medical'
        end as channel_type
    from channel_data
),

final as (
    select
        -- Generate surrogate key without dbt_utils
        md5(channel_name || '-' || channel_title) as channel_key,
        
        -- Natural keys
        channel_name,
        channel_title,
        
        -- Channel categorization
        channel_type,
        
        -- Time statistics
        first_post_date,
        last_post_date,
        
        -- Activity metrics
        total_posts,
        posts_with_images,
        round(posts_with_images * 100.0 / nullif(total_posts, 0), 2) as image_percentage,
        
        -- Engagement metrics
        round(avg_views::numeric, 2) as avg_views,
        round(avg_forwards::numeric, 2) as avg_forwards,
        round(avg_engagement_score::numeric, 2) as avg_engagement_score,
        
        -- Calculated fields
        case 
            when total_posts > 1000 then 'High Activity'
            when total_posts > 100 then 'Medium Activity'
            else 'Low Activity'
        end as activity_level
        
    from channel_categories
)

select * from final
  );
  