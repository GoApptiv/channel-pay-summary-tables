{{config(materialized = 'table')}}
SELECT * FROM {{ref('inter_cp_monthly_department_headquarter_customer_type_order_summary')}}
