WITH 

departments AS (

   SELECT
        user_id,
        dept_id,
        hq_id,
        id,
        derived_customer_type,
        status        
          
   FROM
    `goapptiv-data-lake.datastream_cpay_production_sql_channelpay_production.channelpay_user_departments`
)

SELECT * FROM departments
