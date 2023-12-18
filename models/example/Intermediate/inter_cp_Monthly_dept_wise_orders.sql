with 
 {{config(materialized = 'ephemeral')}}

dept as (

SELECT
 department,
 c.organization,
 FORMAT_TIMESTAMP('%Y-%m-01',o.created_at) month_year,
 COUNT(DISTINCT o.receiver_id) unique_retailers,
 COUNT(DISTINCT o.id) orderCount,
 SUM(o.amount) orderAmount
 
FROM
  {{ref('stg_channelpay_prod_channelpay_orders')}} o
JOIN
  {{ref('stg_channelpay_prod_channelpay_product_offers')}} po
ON
 po.id =o.offer_id
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
WHERE
 ud.status ='active'
 AND hh.brick_status ='active'
GROUP BY
 department,
 organization,
 month_year
)

SELECT * FROM dept
