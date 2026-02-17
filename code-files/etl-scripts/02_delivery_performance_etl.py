import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

# Initialize contexts
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

# S3 paths
RAW_BUCKET = args['S3_RAW_BUCKET']
PROCESSED_BUCKET = args['S3_PROCESSED_BUCKET']

try:
    logger.info("Starting Sales & Revenue ETL Pipeline...")
    
    # Read from S3 paths
    orders_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/orders/*.csv")
    
    order_items_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/order-items/*.csv")
    
    products_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/products/*.csv")
    
    logger.info(f"Orders: {orders_df.count()}, Order Items: {order_items_df.count()}, Products: {products_df.count()}")
    
    # Data cleaning with proper timestamp parsing
    orders_df = orders_df.dropDuplicates(["order_id"]) \
        .filter(col("order_id").isNotNull()) \
        .withColumn("order_date_ts", to_timestamp("order_date"))
    
    order_items_df = order_items_df.filter(
        (col("order_id").isNotNull()) & 
        (col("product_id").isNotNull()) &
        (col("quantity") > 0)
    )
    
    products_df = products_df.dropDuplicates(["product_id"]) \
        .filter(col("product_id").isNotNull())
    
    print("Data cleaning completed")
    logger.info("Data cleaning completed successfully")
    
    # Join datasets
    sales_df = order_items_df \
        .join(orders_df, "order_id", "inner") \
        .join(products_df, "product_id", "inner")
    
    # Calculate derived metrics
    sales_df = sales_df.withColumn(
        "line_total", 
        col("quantity") * col("unit_price")
    )
    
    sales_df = sales_df.withColumn(
        "profit_margin",
        (col("price") - (col("price") * (1 - col("margin_percentage") / 100))) * col("quantity")
    )
    
    # Add date partitions
    sales_df = sales_df \
        .withColumn("year", year("order_date_ts")) \
        .withColumn("month", month("order_date_ts")) \
        .withColumn("day", dayofmonth("order_date_ts"))
    
    print("Metrics calculation completed")
    logger.info("Metrics calculation completed successfully")
    
    # Write to processed zone
    processed_path = f"s3://{PROCESSED_BUCKET}/processed/sales_transactions/"
    sales_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(processed_path)
    
    logger.info(f"Processed sales data written to {processed_path}")
    
    print("Sales & Revenue ETL completed successfully!")
    logger.info("Sales & Revenue ETL completed successfully!")
    
    job.commit()

except Exception as e:
    print(f"Error in Sales Revenue ETL: {str(e)}")
    logger.error(f"Error in Sales Revenue ETL: {str(e)}")
    raise