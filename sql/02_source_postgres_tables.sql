

CREATE TABLE IF NOT EXISTS customers (
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

CREATE TABLE IF NOT EXISTS products (
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

