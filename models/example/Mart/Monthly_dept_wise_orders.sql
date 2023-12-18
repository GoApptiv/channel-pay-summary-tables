{{config(materialized = 'table')}}
SELECT * FROM {{ref('inter_cp_Monthly_dept_wise_orders')}}
