import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_RAW_BUCKET',
    'S3_PROCESSED_BUCKET',
    'S3_CURATED_BUCKET'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RAW_BUCKET = args['S3_RAW_BUCKET']
PROCESSED_BUCKET = args['S3_PROCESSED_BUCKET']
CURATED_BUCKET = args['S3_CURATED_BUCKET']

try:
    logger.info("Starting Customer Feedback ETL Pipeline...")
    print("Starting Customer Feedback ETL Pipeline...")
    
    # ✅ Read data with correct paths
    feedback_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/feedback/*.csv")
    
    customers_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/customers/*.csv")
    
    orders_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/orders/*.csv")
    
    logger.info(f"Feedback: {feedback_df.count()}, Customers: {customers_df.count()}, Orders: {orders_df.count()}")
    
    # ✅ Rating icon mapping
    rating_mapping = {
        1: "⭐",
        2: "⭐⭐", 
        3: "⭐⭐⭐",
        4: "⭐⭐⭐⭐",
        5: "⭐⭐⭐⭐⭐"
    }
    
    from pyspark.sql.types import IntegerType, StringType, StructType, StructField
    rating_schema = StructType([
        StructField("rating", IntegerType(), True),
        StructField("rating_icon", StringType(), True)
    ])
    rating_icon_df = spark.createDataFrame(
        [(k, v) for k, v in rating_mapping.items()],
        rating_schema
    )
    
    print("✅ Data sources loaded")
    
    # ✅ Clean feedback data
    feedback_df = feedback_df \
        .dropDuplicates(["feedback_id"]) \
        .filter(col("feedback_id").isNotNull()) \
        .withColumn("feedback_date_ts", to_timestamp("feedback_date"))
    
    # Normalize sentiment values
    feedback_df = feedback_df.withColumn(
        "sentiment_normalized",
        when(lower(col("sentiment")).isin("positive", "good", "excellent"), "Positive")
        .when(lower(col("sentiment")).isin("negative", "bad", "poor"), "Negative")
        .when(lower(col("sentiment")).isin("neutral", "average", "okay"), "Neutral")
        .otherwise("Unknown")
    )
    
    # Calculate sentiment score
    feedback_df = feedback_df.withColumn(
        "sentiment_score",
        when(col("sentiment_normalized") == "Positive", 1)
        .when(col("sentiment_normalized") == "Neutral", 0)
        .when(col("sentiment_normalized") == "Negative", -1)
        .otherwise(0)
    )
    
    print("✅ Sentiment analysis completed")
    logger.info("Sentiment analysis completed")
    
    # ✅ Join with rating icons
    feedback_df = feedback_df.join(rating_icon_df, "rating", "left")
    
    # Join with customers and orders
    feedback_enriched = feedback_df \
        .join(customers_df.select("customer_id", "area", "customer_segment"), "customer_id", "left") \
        .join(orders_df.select("order_id", "order_date", "delivery_status"), "order_id", "left")
    
    # Add date partitions
    feedback_enriched = feedback_enriched \
        .withColumn("year", year("feedback_date_ts")) \
        .withColumn("month", month("feedback_date_ts")) \
        .withColumn("day", dayofmonth("feedback_date_ts"))
    
    # Write to processed zone
    processed_path = f"s3://{PROCESSED_BUCKET}/processed/customer_feedback/"
    feedback_enriched.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(processed_path)
    
    logger.info(f"Processed feedback data written to {processed_path}")
    
    # ========================================================================
    # CURATED ZONE - ONLY KEEP: area_feedback_metrics
    # ========================================================================
    
    # 1. Area-wise Feedback Metrics (KEEP)
    print("✅ Aggregating area feedback metrics...")
    area_feedback = feedback_enriched.groupBy("area") \
        .agg(
            count("feedback_id").alias("total_feedback"),
            avg("rating").alias("avg_rating"),
            sum(when(col("sentiment_normalized") == "Positive", 1).otherwise(0)).alias("positive_count"),
            sum(when(col("sentiment_normalized") == "Negative", 1).otherwise(0)).alias("negative_count"),
            avg("sentiment_score").alias("avg_sentiment_score")
        ) \
        .withColumn("satisfaction_rate", col("positive_count") / col("total_feedback") * 100) \
        .orderBy(desc("avg_rating"))
    
    area_feedback.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(f"s3://{CURATED_BUCKET}/curated/area_feedback_metrics/")
    
    logger.info("✅ Area feedback metrics written")
    
    print("✅ Customer Feedback ETL completed successfully!")
    logger.info("✅ Customer Feedback ETL completed successfully!")
    
    job.commit()

except Exception as e:
    print(f"❌ Error in Customer Feedback ETL: {str(e)}")
    logger.error(f"❌ Error in Customer Feedback ETL: {str(e)}")
    raise