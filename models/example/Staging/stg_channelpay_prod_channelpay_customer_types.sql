WITH 

customer_types AS (

  SELECT
      name as customer_type, 
      id            
          
   FROM
      `goapptiv-data-lake.datastream_cpay_production_sql_channelpay_production.channelpay_customer_types` 
)

SELECT * FROM customer_types
