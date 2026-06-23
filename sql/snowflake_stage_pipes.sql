-- Fix stages and pipes for S3 auto-ingest into RAW tables
-- Co-authored with CoCo

-- =====================================================
-- SETUP
-- =====================================================

DROP SCHEMA IF EXISTS LAND;
USE DATABASE SNOWFLAKE_WH;
CREATE SCHEMA IF NOT EXISTS LAND;

-- =====================================================
-- STEP 1: CREATE ALL STAGES FIRST
-- =====================================================

CREATE OR REPLACE STAGE SNOWFLAKE_WH.LAND.CUSTOMERS_RAW
STORAGE_INTEGRATION = S3_INT
URL = 's3://jeet-project-raw-layer/raw/customers/'
FILE_FORMAT = (TYPE = PARQUET, USE_LOGICAL_TYPE = FALSE);

CREATE OR REPLACE STAGE SNOWFLAKE_WH.LAND.PRODUCTS_RAW
STORAGE_INTEGRATION = S3_INT
URL = 's3://jeet-project-raw-layer/raw/products/'
FILE_FORMAT = (TYPE = PARQUET, USE_LOGICAL_TYPE = FALSE);

CREATE OR REPLACE STAGE SNOWFLAKE_WH.LAND.ORDERS_RAW
STORAGE_INTEGRATION = S3_INT
URL = 's3://jeet-project-raw-layer/raw/orders/'
FILE_FORMAT = (TYPE = PARQUET, USE_LOGICAL_TYPE = FALSE);

-- Verify files are visible in each stage
LIST @SNOWFLAKE_WH.LAND.CUSTOMERS_RAW;
LIST @SNOWFLAKE_WH.LAND.PRODUCTS_RAW;
LIST @SNOWFLAKE_WH.LAND.ORDERS_RAW;

-- =====================================================
-- STEP 2: CREATE PIPES (after stages exist)
-- =====================================================

CREATE OR REPLACE PIPE SNOWFLAKE_WH.LAND.CUSTOMERS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO SNOWFLAKE_WH.raw_table.CUSTOMERS
FROM @SNOWFLAKE_WH.LAND.CUSTOMERS_RAW
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE SNOWFLAKE_WH.LAND.PRODUCTS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO SNOWFLAKE_WH.raw_table.PRODUCTS
FROM @SNOWFLAKE_WH.LAND.PRODUCTS_RAW
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE SNOWFLAKE_WH.LAND.ORDERS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO SNOWFLAKE_WH.raw_table.ORDERS
FROM @SNOWFLAKE_WH.LAND.ORDERS_RAW
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- =====================================================
-- STEP 3: REFRESH PIPES (load existing S3 files)
-- =====================================================

ALTER PIPE SNOWFLAKE_WH.LAND.CUSTOMERS_PIPE REFRESH;
ALTER PIPE SNOWFLAKE_WH.LAND.PRODUCTS_PIPE REFRESH;
ALTER PIPE SNOWFLAKE_WH.LAND.ORDERS_PIPE REFRESH;

-- =====================================================
-- STEP 4: GET NOTIFICATION CHANNEL FOR S3 CONFIG
-- (Update your S3 bucket event notification with this ARN)
-- =====================================================

DESC PIPE SNOWFLAKE_WH.LAND.CUSTOMERS_PIPE;
DESC PIPE SNOWFLAKE_WH.LAND.PRODUCTS_PIPE;
DESC PIPE SNOWFLAKE_WH.LAND.ORDERS_PIPE;

-- =====================================================
-- VALIDATE: CHECK PIPE STATUS
-- =====================================================
SELECT SYSTEM$PIPE_STATUS('SNOWFLAKE_WH.LAND.CUSTOMERS_PIPE');
SELECT SYSTEM$PIPE_STATUS('SNOWFLAKE_WH.LAND.PRODUCTS_PIPE');
SELECT SYSTEM$PIPE_STATUS('SNOWFLAKE_WH.LAND.ORDERS_PIPE');

-- =====================================================
-- VALIDATE: CHECK DATA IN TARGET TABLES
-- =====================================================
SELECT * FROM raw_table.CUSTOMERS;
SELECT * FROM raw_table.PRODUCTS;
SELECT * FROM raw_table.ORDERS;