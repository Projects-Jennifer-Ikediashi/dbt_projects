{{ config(materialized='table') }}

WITH order_totals AS (
    SELECT
        customer_id,
        SUM(p.price * o.quantity) AS total_spent,
        COUNT(o.order_id) AS total_orders
    FROM
        {{ ref('orders') }} AS o
    JOIN
        {{ ref('products') }} AS p
    ON
        o.product_id = p.product_id
    WHERE
        o.order_status = 'completed'
    GROUP BY
        customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.signup_date,
    ot.total_spent,
    ot.total_orders,
    (CURRENT_DATE - c.signup_date) AS customer_age_days,
    ot.total_spent / ot.total_orders AS avg_order_value, -- New Column: Average Order Value
    ot.total_spent / (CURRENT_DATE - c.signup_date) AS avg_spent_per_day
FROM
    {{ ref('customers') }} AS c
LEFT JOIN
    order_totals AS ot
ON
    c.customer_id = ot.customer_id
