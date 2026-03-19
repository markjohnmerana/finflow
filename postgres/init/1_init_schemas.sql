-- ============================================
-- FinFlow Database Initialization
-- ============================================

-- Create schemas for Medallion Architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO finflow_user;
GRANT ALL PRIVILEGES ON SCHEMA silver TO finflow_user;
GRANT ALL PRIVILEGES ON SCHEMA gold TO finflow_user;

-- Confirm
SELECT 'FinFlow schemas initialized successfully' AS status;