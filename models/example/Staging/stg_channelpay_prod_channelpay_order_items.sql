WITH 

items AS (

   SELECT
       product,
       quantity,
       unit_price,
       order_id
          
   FROM
    `goapptiv-data-lake.datastream_cpay_production_sql_channelpay_production.channelpay_order_items` 
)

SELECT * FROM items
