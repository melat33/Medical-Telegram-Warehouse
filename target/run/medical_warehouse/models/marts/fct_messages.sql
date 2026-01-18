
      
        
            delete from "medical_warehouse"."analytics_analytics"."fct_messages"
            where (
                message_id) in (
                select (message_id)
                from "fct_messages__dbt_tmp165017493557"
            );

        
    

    insert into "medical_warehouse"."analytics_analytics"."fct_messages" ("fact_key", "message_id", "channel_key", "date_key", "message_text", "message_length", "message_length_category", "view_count", "forward_count", "engagement_score", "has_image", "image_path", "channel_name", "channel_title", "message_date", "hour_of_day", "extracted_at", "loaded_at", "views_per_forward", "posting_time_category", "popularity_level")
    (
        select "fact_key", "message_id", "channel_key", "date_key", "message_text", "message_length", "message_length_category", "view_count", "forward_count", "engagement_score", "has_image", "image_path", "channel_name", "channel_title", "message_date", "hour_of_day", "extracted_at", "loaded_at", "views_per_forward", "posting_time_category", "popularity_level"
        from "fct_messages__dbt_tmp165017493557"
    )
  