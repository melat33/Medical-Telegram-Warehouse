-- Simple database initialization
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Grant privileges
GRANT ALL PRIVILEGES ON SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA staging TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA marts TO postgres;

-- Create extension for text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;
