WITH 

channelpay_clients AS (

  SELECT
      name as organization,  
      id          
          
   FROM
      `goapptiv-data-lake.datastream_cpay_production_sql_channelpay_production.channelpay_clients` 
)

SELECT * FROM channelpay_clients
