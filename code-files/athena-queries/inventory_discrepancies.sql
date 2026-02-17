SELECT 
    product_id,
    product_name,
    category,
    discrepancy_occurrences,
    total_stock_variance,
    total_damaged_variance,
    ROUND(total_impact, 2) AS total_impact,
    max_severity,
    ROUND(total_impact / discrepancy_occurrences, 2) AS avg_impact_per_occurrence
FROM curated_product_inventory_discrepancies
WHERE total_impact > 0
ORDER BY total_impact DESC
LIMIT 15;