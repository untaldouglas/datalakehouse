-- ============================================================
-- PostgreSQL init: create analytics user and semantic database
-- Runs BEFORE 01_semantic_schema.sql (alphabetical order)
-- ============================================================

-- Create the analytics user if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analytics') THEN
    CREATE USER analytics WITH PASSWORD 'analytics_secret_2024';
  END IF;
END
$$;

-- Create the semantic database
CREATE DATABASE universidad_analytics OWNER analytics;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE universidad_analytics TO analytics;
