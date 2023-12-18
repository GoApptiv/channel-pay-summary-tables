{{config(materialized = 'table')}}
SELECT * FROM {{ref('inter_cp_Monthly_dept_hq_cus_wise_orders')}}
