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
    city                VARCHAR(100),
    country             VARCHAR(100),
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











INSERT INTO postgres_table.customers
(customer_id, first_name, last_name, email, phone, city, country, created_at, updated_at)
VALUES
(16,'Leonardo','DiCaprio','leo.dicaprio@gmail.com','9876543225','Bangalore','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(17,'Jennifer','Lawrence','jennifer.lawrence@gmail.com','9876543226','Mumbai','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(18,'Brad','Pitt','brad.pitt@gmail.com','9876543227','Chennai','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(19,'Natalie','Portman','natalie.portman@gmail.com','9876543228','Pune','India',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);

INSERT INTO postgres_table.products
(product_id, product_name, category, sub_category, brand, price, cost, created_at, updated_at)
VALUES
(116,'Nothing Phone 3','Electronics','Smartphones','Nothing',54999,42000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(117,'Xiaomi 16 Ultra','Electronics','Smartphones','Xiaomi',79999,63000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(118,'Asus ROG Strix G18','Electronics','Laptop','Asus',149999,120000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(119,'LG OLED C5','Electronics','Television','LG',129999,105000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(120,'Bose QuietComfort Ultra','Electronics','Headphones','Bose',34999,26000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(121,'Garmin Fenix 8','Electronics','Smartwatch','Garmin',89999,72000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(122,'Lenovo Tab Extreme','Electronics','Tablet','Lenovo',69999,54000,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);

INSERT INTO postgres_table.orders
(order_id, customer_id, product_id, order_date, quantity, unit_price,
 discount, total_amount, order_status, payment_method,
 created_at, updated_at)
VALUES
(1016,16,116,CURRENT_TIMESTAMP,1,54999.00,2.00,53899.02,'DELIVERED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1017,17,117,CURRENT_TIMESTAMP,1,79999.00,3.00,77599.03,'SHIPPED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1018,18,118,CURRENT_TIMESTAMP,1,149999.00,5.00,142499.05,'PROCESSING','NETBANKING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1019,19,119,CURRENT_TIMESTAMP,1,129999.00,4.00,124799.04,'DELIVERED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1020,1,120,CURRENT_TIMESTAMP,2,34999.00,2.50,68248.05,'SHIPPED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),

(1021,2,121,CURRENT_TIMESTAMP,1,89999.00,3.00,87299.03,'PROCESSING','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1022,3,122,CURRENT_TIMESTAMP,1,69999.00,1.00,69299.01,'DELIVERED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1023,4,116,CURRENT_TIMESTAMP,1,54999.00,2.00,53899.02,'CANCELLED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1024,5,117,CURRENT_TIMESTAMP,2,79999.00,4.00,153598.08,'DELIVERED','NETBANKING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1025,6,118,CURRENT_TIMESTAMP,1,149999.00,5.00,142499.05,'SHIPPED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),

(1026,7,119,CURRENT_TIMESTAMP,1,129999.00,0.00,129999.00,'PROCESSING','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1027,8,120,CURRENT_TIMESTAMP,3,34999.00,3.50,101322.11,'DELIVERED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1028,9,121,CURRENT_TIMESTAMP,1,89999.00,2.50,87749.03,'SHIPPED','NETBANKING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1029,10,122,CURRENT_TIMESTAMP,2,69999.00,3.00,135798.06,'DELIVERED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1030,11,116,CURRENT_TIMESTAMP,1,54999.00,1.50,54174.02,'PROCESSING','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),

(1031,12,117,CURRENT_TIMESTAMP,1,79999.00,4.50,76399.05,'DELIVERED','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1032,13,118,CURRENT_TIMESTAMP,1,149999.00,6.00,140999.06,'SHIPPED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1033,14,119,CURRENT_TIMESTAMP,1,129999.00,5.00,123499.05,'DELIVERED','NETBANKING',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1034,15,120,CURRENT_TIMESTAMP,2,34999.00,2.00,68598.04,'PROCESSING','UPI',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
(1035,16,121,CURRENT_TIMESTAMP,1,89999.00,3.50,86849.04,'DELIVERED','CARD',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);