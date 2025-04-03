CREATE DATABASE IF NOT EXISTS tb_101;
USE DATABASE tb_101;

CREATE OR REPLACE FILE FORMAT tb_101.public.csv_ff
type = 'csv';

CREATE OR REPLACE STAGE tb_101.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
URL = 's3://sfquickstarts/frostbyte_tastybytes/'
FILE_FORMAT = tb_101.public.csv_ff;

CREATE OR REPLACE TABLE tb_101.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);


COPY INTO tb_101.raw_pos.order_header
    FROM @tb_101.public.s3load/raw_pos/order_header/
    SIZE_LIMIT = 2000000;

CREATE OR REPLACE TABLE tb_101.raw_pos.incremental_order_header LIKE tb_101.raw_pos.order_header;

ALTER TABLE tb_101.raw_pos.INCREMENTAL_ORDER_HEADER SET CHANGE_TRACKING = TRUE;
GRANT SELECT ON TABLE tb_101.raw_pos.order_header TO ROLE riotx_cdc;
GRANT SELECT ON TABLE tb_101.raw_pos.incremental_order_header TO ROLE riotx_cdc;

INSERT INTO tb_101.raw_pos.incremental_order_header
 select * from tb_101.raw_pos.order_header
 LIMIT 100 OFFSET 0;
