with
 {{config(materialized = 'ephemeral')}}

ord as (
SELECT
    department,
    c.organization,
    FORMAT_TIMESTAMP('%Y-%m-01',o.created_at) month_year,
    i.product,
    sum(CAST(i.quantity as integer)) quantity,
    SUM(ROUND(i.unit_price * CAST(i.quantity as integer),2)) order_amount,
    COUNT(DISTINCT o.receiver_id) unique_retailers,
    COUNT(*)orders
FROM
  {{ref('stg_channelpay_prod_channelpay_orders')}} o
JOIN
  {{ref('stg_channelpay_prod_channelpay_product_offers')}} po
ON
 po.id=o.offer_id
JOIN
  {{ref('stg_channelpay_prod_channelpay_clients')}} c
ON
 c.id=po.org_id
JOIN
  {{ref('stg_channelpay_prod_channelpay_user_departments')}} ud
ON
 o.receiver_id=ud.user_id
 AND ud.dept_id=po.dept_id
JOIN
 {{ref('stg_channelpay_prod_channelpay_headquarter_hierarchies')}} h
ON
 h.brick_id=ud.hq_id
JOIN
{{ref('stg_channelpay_prod_channelpay_order_items')}} i
ON
 i.order_id=o.id
WHERE
 ud.status='active'
 AND h.brick_status ='active'
GROUP BY
 department,
 c.organization,
 month_year,
 i.product
 )
 
 SELECT * FROM ord
