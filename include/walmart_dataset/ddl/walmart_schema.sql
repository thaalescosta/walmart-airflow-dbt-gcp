-- DDL only: creates the dataset and (empty) tables. Data is loaded by the DAG's GCSToBigQueryOperator tasks.
CREATE SCHEMA IF NOT EXISTS walmart_db;

-- ---------- Tables ----------
CREATE OR REPLACE TABLE walmart_db.customers (
    customer_id INT64,
    first_name STRING(100),
    last_name STRING(100),
    email STRING(255),
    phone STRING(50),
    city STRING(100),
    province STRING(100),
    country STRING(100),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active STRING(1),
    PRIMARY KEY (customer_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE walmart_db.stores (
    store_id INT64,
    store_name STRING(255),
    city STRING(100),
    province STRING(100),
    country STRING(100),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active STRING(1),
    PRIMARY KEY (store_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE walmart_db.products (
    product_id INT64,
    product_name STRING(255),
    category STRING(100),
    brand STRING(100),
    price NUMERIC(10,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active STRING(1),
    PRIMARY KEY (product_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE walmart_db.employees (
    employee_id INT64,
    store_id INT64,
    first_name STRING(100),
    last_name STRING(100),
    email STRING(255),
    job_title STRING(100),
    salary NUMERIC(10,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active STRING(1),
    PRIMARY KEY (employee_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE walmart_db.orders (
    order_id INT64,
    customer_id INT64,
    store_id INT64,
    order_timestamp TIMESTAMP,
    payment_method STRING(50),
    order_status STRING(50),
    total_amount NUMERIC(12,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active STRING(1),
    PRIMARY KEY (order_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE walmart_db.order_items (
    order_item_id INT64,
    order_id INT64,
    product_id INT64,
    quantity INT64,
    unit_price NUMERIC(10,2),
    line_amount NUMERIC(12,2),
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    is_active STRING(1),
    PRIMARY KEY (order_item_id) NOT ENFORCED
);