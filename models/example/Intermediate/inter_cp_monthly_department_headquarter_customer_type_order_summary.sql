With 
 {{config(materialized = 'ephemeral')}}

orders AS (
 SELECT
      dept_code as department,
      c.org_code as organization,
      region,
      area,
      headquarter,
      FORMAT_TIMESTAMP('%Y-%m-01',o.created_at) month_year,
      ud.user_id,
      ct.customer_type,
      COUNT(DISTINCT o.id) orderCount,
      SUM(o.amount) orderAmount
FROM
 {{ref('stg_channelpay_prod_channelpay_orders')}} o
JOIN
 {{ref('stg_channelpay_prod_channelpay_clients')}} c
ON
 c.id=o.org_id
JOIN
{{ref('stg_channelpay_prod_channelpay_user_departments')}} ud
ON
 ud.user_id =o.receiver_id
 AND o.dept_id =ud.dept_id
JOIN
{{ref('stg_channelpay_prod_channelpay_headquarter_hierarchies')}} hh
ON
 hh.brick_id =ud.hq_id
LEFT JOIN
{{ref('stg_channelpay_prod_channelpay_customer_types')}} ct
ON
 ct.id =ud.derived_customer_type
WHERE
 ud.status ='active'
 AND hh.brick_status ='active'
GROUP BY
 dept_code,
 c.org_code,
 region,
 area,
 headquarter,
 month_year,
 user_id,
 ct.customer_type
)
 SELECT * FROM orders
