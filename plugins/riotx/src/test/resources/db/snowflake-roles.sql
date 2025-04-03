
CREATE OR REPLACE ROLE riotx_cdc
COMMENT = 'minimum cdc role for riotx';

GRANT USAGE, OPERATE ON WAREHOUSE compute_wh TO ROLE riotx_cdc;

CREATE OR REPLACE SCHEMA tb_101.raw_pos;
CREATE OR REPLACE SCHEMA tb_101.raw_pos_cdc;

GRANT USAGE ON DATABASE tb_101 TO ROLE riotx_cdc;
GRANT USAGE ON SCHEMA tb_101.raw_pos TO ROLE riotx_cdc;
GRANT USAGE ON SCHEMA tb_101.raw_pos_cdc TO ROLE riotx_cdc;


GRANT SELECT ON FUTURE TABLES IN SCHEMA tb_101.raw_pos_cdc TO ROLE riotx_cdc;
GRANT CREATE TABLE ON SCHEMA tb_101.raw_pos_cdc TO ROLE riotx_cdc;
GRANT CREATE STREAM ON SCHEMA tb_101.raw_pos_cdc TO ROLE riotx_cdc;
GRANT SELECT ON FUTURE STREAMS IN SCHEMA tb_101.raw_pos_cdc TO ROLE riotx_cdc;


CREATE OR REPLACE USER riotx_cdc
    DEFAULT_ROLE = 'riotx_cdc'
    DEFAULT_WAREHOUSE = 'compute_wh'
    PASSWORD = '{{PASSWORD}}';

GRANT ROLE riotx_cdc TO ROLE accountadmin;
GRANT ROLE riotx_cdc TO USER riotx_cdc;
GRANT ROLE accountadmin TO USER riotx_cdc;
