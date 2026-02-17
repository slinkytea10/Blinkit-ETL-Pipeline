-- Campaign ROI Ranking with Efficiency Metrics

SELECT 
    campaign_id,
    campaign_name,
    channel,
    ROUND(total_spend, 2) AS spend,
    ROUND(total_revenue, 2) AS revenue,
    total_conversions,
    ROUND(avg_ctr, 4) AS ctr,
    ROUND(avg_conversion_rate, 4) AS conversion_rate,
    ROUND(overall_roi, 2) AS roi_percentage,
    ROUND(avg_roas, 2) AS roas
FROM curated_campaign_performance_summary
ORDER BY overall_roi DESC;