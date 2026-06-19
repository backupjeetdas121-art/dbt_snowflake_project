-- =====================================================================
-- 02_source_postgres_tables.sql
-- Sample SOURCE schema (Postgres OLTP side).
-- Includes a trigger to auto-maintain `updated_at`, which the
-- incremental extractor (extract/postgres_to_s3.py) relies on.
-- =====================================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id         BIGSERIAL PRIMARY KEY,
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    email               VARCHAR(255) NOT NULL UNIQUE,
    phone               VARCHAR(50),
    address             VARCHAR(255),
    city                VARCHAR(100),
    state               VARCHAR(100),
    country             VARCHAR(100),
    acquisition_channel VARCHAR(50) DEFAULT 'unknown',
    signup_date         DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at          TIMESTAMP NOT NULL DEFAULT now(),
    updated_at          TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id   BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category     VARCHAR(100),
    sub_category VARCHAR(100),
    brand        VARCHAR(100),
    price        NUMERIC(12,2) NOT NULL,
    cost         NUMERIC(12,2) NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id       BIGSERIAL PRIMARY KEY,
    customer_id    BIGINT NOT NULL REFERENCES customers(customer_id),
    product_id     BIGINT NOT NULL REFERENCES products(product_id),
    order_date     TIMESTAMP NOT NULL DEFAULT now(),
    quantity       INTEGER NOT NULL CHECK (quantity >= 0),
    unit_price     NUMERIC(12,2) NOT NULL,
    discount       NUMERIC(5,2) DEFAULT 0,
    total_amount   NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    order_status   VARCHAR(50) NOT NULL DEFAULT 'pending',
    payment_method VARCHAR(50),
    created_at     TIMESTAMP NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Auto-maintain updated_at on every UPDATE.
-- The incremental extractor depends entirely on this column being
-- accurate — without it, changes (e.g. order status changes, customer
-- address edits) would never be picked up by the watermark query.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON products
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Helpful indexes for the incremental extract query
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers (updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated_at  ON products (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at     ON orders (updated_at);
