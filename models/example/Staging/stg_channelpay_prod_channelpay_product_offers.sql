WITH 

offers AS (

   SELECT
       id,
       dept_id,
       org_id
          
   FROM
    `goapptiv-data-lake.datastream_cpay_production_sql_channelpay_production.channelpay_product_offers` 
)

SELECT * FROM offers
