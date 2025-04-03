INSERT INTO tb_101.raw_pos.incremental_order_header
 select * from tb_101.raw_pos.order_header
 LIMIT 1000 OFFSET 100;
