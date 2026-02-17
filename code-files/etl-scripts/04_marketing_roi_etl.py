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
    'S3_PROCESSED_BUCKET'
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

try:
    logger.info("Starting Marketing ROI ETL Pipeline...")
    print("Starting Marketing ROI ETL Pipeline...")
    
    # Read data with correct paths
    marketing_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/marketing/*.csv")
    
    orders_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/orders/*.csv")
    
    logger.info(f"Marketing campaigns: {marketing_df.count()}, Orders: {orders_df.count()}")
    
    # Clean marketing data
    marketing_df = marketing_df \
        .dropDuplicates(["campaign_id", "date"]) \
        .filter(col("campaign_id").isNotNull()) \
        .withColumn("date_ts", to_timestamp("date"))
    
    print("Data cleaning completed")
    
    # Calculate key marketing metrics
    
    # 1. Click-Through Rate (CTR)
    marketing_df = marketing_df.withColumn(
        "ctr",
        when(col("impressions") > 0, (col("clicks") / col("impressions")) * 100).otherwise(0)
    )
    
    # 2. Conversion Rate
    marketing_df = marketing_df.withColumn(
        "conversion_rate",
        when(col("clicks") > 0, (col("conversions") / col("clicks")) * 100).otherwise(0)
    )
    
    # 3. Cost Per Click (CPC)
    marketing_df = marketing_df.withColumn(
        "cpc",
        when(col("clicks") > 0, col("spend") / col("clicks")).otherwise(0)
    )
    
    # 4. Cost Per Acquisition (CPA)
    marketing_df = marketing_df.withColumn(
        "cpa",
        when(col("conversions") > 0, col("spend") / col("conversions")).otherwise(0)
    )
    
    # 5. Return on Ad Spend (ROAS)
    marketing_df = marketing_df.withColumn(
        "roas_calculated",
        when(col("spend") > 0, col("revenue_generated") / col("spend")).otherwise(0)
    )
    
    # 6. ROI Percentage
    marketing_df = marketing_df.withColumn(
        "roi_percentage",
        when(col("spend") > 0, ((col("revenue_generated") - col("spend")) / col("spend")) * 100).otherwise(0)
    )
    
    # 7. Campaign Effectiveness Score
    marketing_df = marketing_df.withColumn(
        "effectiveness_score",
        (col("conversion_rate") * 0.4) + (col("roas_calculated") * 10 * 0.4) + (col("ctr") * 0.2)
    )
    
    print("Marketing metrics calculated")
    logger.info("Marketing metrics calculated successfully")
    
    # Add date partitions
    marketing_df = marketing_df \
        .withColumn("year", year("date_ts")) \
        .withColumn("month", month("date_ts")) \
        .withColumn("day", dayofmonth("date_ts"))
    
    # Write to processed zone
    processed_path = f"s3://{PROCESSED_BUCKET}/processed/marketing_performance/"
    marketing_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(processed_path)
    
    logger.info(f"Processed marketing data written to {processed_path}")
    
    print("Marketing ROI ETL completed successfully!")
    logger.info("Marketing ROI ETL completed successfully!")
    
    job.commit()

except Exception as e:
    print(f"Error in Marketing ROI ETL: {str(e)}")
    logger.error(f"Error in Marketing ROI ETL: {str(e)}")
    raise