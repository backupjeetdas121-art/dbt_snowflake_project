-- =====================================================================
-- 01_stg_tables.sql
-- STG layer: 1:1 landing tables holding ONLY the latest incremental
-- batch loaded via COPY from S3 Parquet (see load/s3_to_redshift.py).
-- These mirror the source Postgres tables for: customers, products, orders
-- =====================================================================

-- ---------------------------------------------------------------------
-- stg.stg_customers
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.stg_customers;
CREATE TABLE stg.stg_customers (
    customer_id         BIGINT,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    phone               VARCHAR(50),
    address             VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(100),
    country             VARCHAR(100),
    acquisition_channel VARCHAR(50),
    signup_date         DATE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

-- ---------------------------------------------------------------------
-- stg.stg_products
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.stg_products;
CREATE TABLE stg.stg_products (
    product_id   BIGINT,
    product_name VARCHAR(255),
    category     VARCHAR(100),
    sub_category VARCHAR(100),
    brand        VARCHAR(100),
    price        NUMERIC(12,2),
    cost         NUMERIC(12,2),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);

-- ---------------------------------------------------------------------
-- stg.stg_orders
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg.stg_orders;
CREATE TABLE stg.stg_orders (
    order_id       BIGINT,
    customer_id    BIGINT,
    product_id     BIGINT,
    order_date     TIMESTAMP,
    quantity       INTEGER,
    unit_price     NUMERIC(12,2),
    discount       NUMERIC(5,2),
    total_amount   NUMERIC(12,2),
    order_status   VARCHAR(50),
    payment_method VARCHAR(50),
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP
);
