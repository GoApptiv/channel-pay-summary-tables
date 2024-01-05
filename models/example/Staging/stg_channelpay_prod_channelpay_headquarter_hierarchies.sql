WITH 

hierarchies AS (
 SELECT
      department,
      customer_service_dept_code as dept_code,
      region,
      area,
      headquarter,
      brick_id,
      brick_status      
          
   FROM
      `goapptiv-data-lake.datastream_cpay_production_sql_channelpay_production.channelpay_headquarter_hierarchies` 
)

SELECT * FROM hierarchies
