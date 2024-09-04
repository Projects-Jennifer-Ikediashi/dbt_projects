{{ config(materialized='table') }}

SELECT
    p.product_id,
    p.product_name,
    SUM(o.quantity) AS total_quantity_sold,
    SUM(p.price * o.quantity) AS total_revenue
FROM
    {{ ref('products') }} AS p
JOIN
    {{ ref('orders') }} AS o
ON
    p.product_id = o.product_id
WHERE
    o.order_status = 'completed'
GROUP BY
    p.product_id, p.product_name
