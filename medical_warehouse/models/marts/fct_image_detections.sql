{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY processed_at) AS detection_id,
    message_id,
    channel_name,
    image_path,
    filename,
    size_kb,
    detection_count,
    category,
    confidence_score,
    business_tags,
    top_objects,
    processed_at AS analysis_timestamp,
    visualization_path,
    CURRENT_TIMESTAMP AS loaded_at
FROM {{ source('processed', 'yolo_results') }}
