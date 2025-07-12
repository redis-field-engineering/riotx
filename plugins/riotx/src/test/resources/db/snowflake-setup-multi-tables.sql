-- Create multiple tables for multi-table import testing
USE DATABASE tb_101;

-- Create table1 with sample product data
CREATE OR REPLACE TABLE tb_101.raw_pos.table1
(
    product_id NUMBER(38,0),
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMBER(10,2),
    created_ts TIMESTAMP_NTZ(9)
);

-- Create table2 with sample customer data  
CREATE OR REPLACE TABLE tb_101.raw_pos.table2
(
    customer_id NUMBER(38,0),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    created_ts TIMESTAMP_NTZ(9)
);

-- Enable change tracking for both tables
ALTER TABLE tb_101.raw_pos.table1 SET CHANGE_TRACKING = TRUE;
ALTER TABLE tb_101.raw_pos.table2 SET CHANGE_TRACKING = TRUE;

-- Grant permissions to CDC role
GRANT SELECT ON TABLE tb_101.raw_pos.table1 TO ROLE riotx_cdc;
GRANT SELECT ON TABLE tb_101.raw_pos.table2 TO ROLE riotx_cdc;

-- Insert sample data into table1
INSERT INTO tb_101.raw_pos.table1 VALUES
(1, 'Widget A', 'Electronics', 29.99, CURRENT_TIMESTAMP()),
(2, 'Gadget B', 'Electronics', 49.99, CURRENT_TIMESTAMP()),
(3, 'Tool C', 'Hardware', 19.99, CURRENT_TIMESTAMP()),
(4, 'Device D', 'Electronics', 79.99, CURRENT_TIMESTAMP()),
(5, 'Part E', 'Hardware', 9.99, CURRENT_TIMESTAMP());

-- Insert sample data into table2
INSERT INTO tb_101.raw_pos.table2 VALUES
(101, 'John Smith', 'john.smith@email.com', '555-0101', CURRENT_TIMESTAMP()),
(102, 'Jane Doe', 'jane.doe@email.com', '555-0102', CURRENT_TIMESTAMP()),
(103, 'Bob Johnson', 'bob.johnson@email.com', '555-0103', CURRENT_TIMESTAMP()),
(104, 'Alice Brown', 'alice.brown@email.com', '555-0104', CURRENT_TIMESTAMP()),
(105, 'Charlie Wilson', 'charlie.wilson@email.com', '555-0105', CURRENT_TIMESTAMP());