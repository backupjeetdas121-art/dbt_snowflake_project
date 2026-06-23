-- =====================================================================
-- 01_stg_tables.sql
-- STG layer: 1:1 landing tables holding ONLY the latest incremental
-- batch loaded via COPY from S3 Parquet (see load/s3_to_redshift.py).
-- These mirror the source Postgres tables for: customers, products, orders
-- =====================================================================

-- ---------------------------------------------------------------------
-- stg.stg_customers
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS postgres_table.customers;
CREATE TABLE postgres_table.customers (
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
DROP TABLE IF EXISTS postgres_table.products;
CREATE TABLE postgres_table.products (
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
-- ---------------------------------------------------------------------  truncate table postgres_table.customers;
  truncate table postgres_table.custtomers;
  truncate table postgres_table.products;
  truncate table postgres_table.orders;
  
  INSERT INTO postgres_table.customers
  (customer_id, first_name, last_name, email, phone, city, country, created_at, updated_at)
  VALUES
  (1,'John','Doe','john.doe@gmail.com','9876543210','Bangalore','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (2,'Jane','Smith','jane.smith@gmail.com','9876543211','Mumbai','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (3,'Robert','Brown','robert.brown@gmail.com','9876543212','Delhi','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (4,'Emily','Davis','emily.davis@gmail.com','9876543213','Chennai','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (5,'Michael','Wilson','michael.wilson@gmail.com','9876543214','Hyderabad','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (6,'Sarah','Taylor','sarah.taylor@gmail.com','9876543215','Pune','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (7,'David','Anderson','david.anderson@gmail.com','9876543216','Kolkata','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (8,'Sophia','Thomas','sophia.thomas@gmail.com','9876543217','Jaipur','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (9,'Daniel','Martin','daniel.martin@gmail.com','9876543218','Ahmedabad','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (10,'Olivia','White','olivia.white@gmail.com','9876543219','Kochi','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
  
  INSERT INTO postgres_table.products
  (product_id, product_name, category, sub_category, brand, price, cost, created_at, updated_at)
  VALUES
  (101,'iPhone 15','Electronics','Smartphones','Apple',79999,65000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (102,'Galaxy S24','Electronics','Smartphones','Samsung',74999,62000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (103,'MacBook Air M3','Electronics','Laptops','Apple',114999,95000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (104,'Inspiron 15','Electronics','Laptops','Dell',65999,54000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (105,'WH-1000XM5','Electronics','Headphones','Sony',24999,18000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (106,'AirPods Pro','Electronics','Earbuds','Apple',24900,18000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (107,'Watch 7','Electronics','Smartwatch','Samsung',29999,22000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (108,'iPad Air','Electronics','Tablet','Apple',59999,47000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (109,'Bravia 55','Electronics','Television','Sony',84999,69000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  (110,'ThinkPad E14','Electronics','Laptop','Lenovo',72999,58000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
  
  INSERT INTO postgres_table.orders
  (order_id, customer_id, product_id, order_date, quantity, unit_price,
   discount, total_amount, order_status, payment_method,
   created_at, updated_at)
  VALUES
  (1001,1,101,CURRENT_TIMESTAMP,1,79999.00,0.00,79999.00,'DELIVERED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1002,2,102,CURRENT_TIMESTAMP,1,74999.00,2.50,73124.03,'DELIVERED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1003,3,103,CURRENT_TIMESTAMP,1,114999.00,5.00,109249.05,'SHIPPED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1004,4,104,CURRENT_TIMESTAMP,2,65999.00,3.00,128038.06,'PROCESSING','NETBANKING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1005,5,105,CURRENT_TIMESTAMP,1,24999.00,4.00,23999.04,'DELIVERED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1006,6,106,CURRENT_TIMESTAMP,2,24900.00,1.50,49053.00,'SHIPPED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1007,7,107,CURRENT_TIMESTAMP,1,29999.00,5.00,28499.05,'DELIVERED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1008,8,108,CURRENT_TIMESTAMP,1,59999.00,2.00,58799.02,'PROCESSING','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1009,9,109,CURRENT_TIMESTAMP,1,84999.00,0.00,84999.00,'CANCELLED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
  
  (1010,10,110,CURRENT_TIMESTAMP,1,72999.00,4.50,69714.05,'DELIVERED','NETBANKING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
  






INSERT INTO postgres_table.orders
(order_id, customer_id, product_id, order_date, quantity, unit_price,
 discount, total_amount, order_status, payment_method,
 created_at, updated_at)
VALUES
(1011,11,111,CURRENT_TIMESTAMP,1,69999.00,2.00,68599.02,'PROCESSING','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),

(1012,12,112,CURRENT_TIMESTAMP,1,79999.00,3.00,77599.03,'DELIVERED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),

(1013,13,113,CURRENT_TIMESTAMP,2,34999.00,1.50,68948.03,'SHIPPED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),

(1014,14,114,CURRENT_TIMESTAMP,1,99999.00,5.00,94999.05,'PROCESSING','NETBANKING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),

(1015,15,115,CURRENT_TIMESTAMP,1,89999.00,4.00,86399.04,'DELIVERED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);

INSERT INTO postgres_table.products
(product_id, product_name, category, sub_category, brand, price, cost, created_at, updated_at)
VALUES
(111,'OnePlus 13','Electronics','Smartphones','OnePlus',69999,55000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(112,'Pixel 10','Electronics','Smartphones','Google',79999,62000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(113,'Galaxy Watch 8','Electronics','Smartwatch','Samsung',34999,26000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(114,'iPad Pro','Electronics','Tablet','Apple',99999,78000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(115,'Surface Laptop','Electronics','Laptop','Microsoft',89999,70000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);


INSERT INTO postgres_table.customers
(customer_id, first_name, last_name, email, phone, city, country, created_at, updated_at)
VALUES
(11,'Chris','Evans','chris.evans@gmail.com','9876543220','Bangalore','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(12,'Emma','Stone','emma.stone@gmail.com','9876543221','Mumbai','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(13,'Ryan','Gosling','ryan.gosling@gmail.com','9876543222','Pune','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(14,'Scarlett','Johansson','scarlett@gmail.com','9876543223','Delhi','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(15,'Tom','Holland','tom.holland@gmail.com','9876543224','Hyderabad','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);


