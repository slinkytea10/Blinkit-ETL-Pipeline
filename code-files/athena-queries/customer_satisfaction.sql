-- Customer Satisfaction by Area with Feedback Depth

SELECT 
    area,
    total_feedback,
    ROUND(avg_rating, 2) AS avg_rating,
    positive_count,
    negative_count,
    ROUND(satisfaction_rate * 100, 2) AS satisfaction_percentage,
    ROUND(avg_sentiment_score, 2) AS sentiment_score,
    ROUND((positive_count::float / total_feedback) * 100, 2) AS positive_feedback_percentage
FROM curated_area_feedback_metrics
ORDER BY satisfaction_rate DESC;