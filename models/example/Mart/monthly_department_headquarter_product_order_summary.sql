{{config(materialized = 'table')}}
SELECT * FROM {{ref('inter_cp_monthly_department_headquarter_product_order_summary')}}
