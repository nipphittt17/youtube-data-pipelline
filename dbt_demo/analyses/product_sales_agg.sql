
-- LAG() → previous month's sales
-- LEAD() → next month's sales
-- SUM() OVER() → cumulative sales
-- AVG() OVER() → rolling 3-month average
-- RANK() → rank platforms by monthly revenue
-- DENSE_RANK() → rank regions/platforms
-- FIRST_VALUE() → first month's revenue
-- LAST_VALUE() → latest revenue with the correct window frame

SELECT
    DATE_TRUNC('MONTH', order_at) AS month,
    platform,
    SUM(item_price_after_discount) AS revenue
FROM {{ ref('fact_order') }}t_order
GROUP BY 1, 2;