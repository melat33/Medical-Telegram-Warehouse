-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create raw table for Telegram messages
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    id SERIAL PRIMARY KEY,
    message_id INTEGER,
    channel_name VARCHAR(255),
    channel_title VARCHAR(255),
    message_date TIMESTAMP,
    message_text TEXT,
    has_media BOOLEAN,
    image_path TEXT,
    views INTEGER,
    forwards INTEGER,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB,
    UNIQUE(message_id, channel_name)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_telegram_channel ON raw.telegram_messages(channel_name);
CREATE INDEX IF NOT EXISTS idx_telegram_date ON raw.telegram_messages(message_date);
CREATE INDEX IF NOT EXISTS idx_telegram_views ON raw.telegram_messages(views DESC);