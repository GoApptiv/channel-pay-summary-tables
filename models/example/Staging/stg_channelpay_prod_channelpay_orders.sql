WITH 

orders AS (

SELECT
     created_at,
     id,
     amount,
     dept_id,
     org_id,
     receiver_id
          
    FROM
       `goapptiv-data-lake.datastream_cpay_production_sql_channelpay_production.channelpay_orders` 
)

SELECT * FROM orders
