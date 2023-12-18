With 
 {{config(materialized = 'ephemeral')}}

od AS (
 SELECT
      department,
      c.organization,
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
 {{ref('stg_channelpay_prod_channelpay_product_offers')}} po
ON
 po.id = o.offer_id
JOIN
 {{ref('stg_channelpay_prod_channelpay_clients')}} c
ON
 c.id=po.org_id
JOIN
{{ref('stg_channelpay_prod_channelpay_user_departments')}} ud
ON
 ud.user_id =o.receiver_id
 AND po.dept_id =ud.dept_id
JOIN
{{ref('stg_channelpay_prod_channelpay_headquarter_hierarchies')}} hh
ON
 hh.brick_id =ud.hq_id
LEFT JOIN
{{ref('stg_channelpay_prod_channelpay_customer_types')}} ct
ON
 ct.id =ud.customer_type_id
WHERE
 ud.status ='active'
 AND hh.brick_status ='active'
GROUP BY
 department,
 c.organization,
 region,
 area,
 headquarter,
 month_year,
 user_id,
 ct.customer_type
)
 SELECT * FROM od
