{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS total_orders,
    SUM(p.price * o.quantity) AS total_spent
FROM
    {{ ref('customers') }} AS c
JOIN
    {{ ref('orders') }} AS o
ON
    c.customer_id = o.customer_id
JOIN
    {{ ref('products') }} AS p
ON
    o.product_id = p.product_id
WHERE
    o.order_status = 'completed'
GROUP BY
    c.customer_id, c.customer_name
