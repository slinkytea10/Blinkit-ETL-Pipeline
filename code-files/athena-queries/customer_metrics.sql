SELECT 
    category,
    total_orders,
    unique_customers,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(total_profit, 2) AS total_profit,
    ROUND(avg_order_value, 2) AS avg_order_value,
    ROUND((total_revenue / unique_customers), 2) AS revenue_per_customer,
    ROUND((total_profit / unique_customers), 2) AS profit_per_customer,
    ROUND((CAST(total_orders AS DOUBLE) / unique_customers), 2) AS avg_orders_per_customer,
    ROUND(((total_profit / total_revenue) * 100), 2) AS profit_margin_percentage
FROM curated_revenue_by_category
ORDER BY total_profit DESC;