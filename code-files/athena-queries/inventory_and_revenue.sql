-- Categories with High Inventory Impact vs. Revenue Performance

SELECT 
    c.category,
    c.total_impact AS inventory_impact_loss,
    c.affected_products,
    c.avg_stock_variance,
    c.avg_damaged_variance,
    r.total_revenue,
    r.total_profit,
    r.total_orders,
    ROUND((c.total_impact / r.total_revenue) * 100, 2) AS impact_percentage_of_revenue
FROM curated_category_inventory_impact c
LEFT JOIN curated_revenue_by_category r ON c.category = r.category
ORDER BY c.total_impact DESC;