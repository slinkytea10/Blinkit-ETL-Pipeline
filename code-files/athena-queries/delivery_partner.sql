-- Delivery Partner Performance Scorecard

SELECT 
    delivery_partner_id,
    total_deliveries,
    delayed_deliveries,
    ROUND(delay_percentage, 2) AS delay_percentage,
    ROUND(avg_delivery_time, 2) AS avg_delivery_time_hours,
    ROUND(avg_distance, 2) AS avg_distance_km,
    CASE 
        WHEN delay_percentage > 10 THEN 'High Risk'
        WHEN delay_percentage > 5 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS performance_tier
FROM curated_delivery_partner_performance
ORDER BY delay_percentage DESC;