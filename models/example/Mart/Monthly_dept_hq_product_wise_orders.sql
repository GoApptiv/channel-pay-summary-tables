{{config(materialized = 'table')}}
SELECT * FROM {{ref('inter_cp_Monthly_dept_hq_product_wise_orders')}}
