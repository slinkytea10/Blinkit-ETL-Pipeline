import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
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
    logger.info("Starting Inventory Reconciliation ETL Pipeline...")
    
    from pyspark.sql.functions import input_file_name
    
    all_inventory = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/inventory/*.csv") \
        .withColumn("source_file", input_file_name())
    
    inventory_old = all_inventory \
        .filter(~col("source_file").contains("inventoryNew")) \
        .drop("source_file")
    
    inventory_new = all_inventory \
        .filter(col("source_file").contains("inventoryNew")) \
        .drop("source_file")
    
    products_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3://{RAW_BUCKET}/products/*.csv")
    
    inventory_old = inventory_old \
        .dropDuplicates(["product_id", "date"]) \
        .filter(col("product_id").isNotNull() & col("date").isNotNull()) \
        .withColumnRenamed("stock_received", "old_stock_received") \
        .withColumnRenamed("damaged_stock", "old_damaged_stock") \
        .withColumn("date_ts", to_date(col("date")))
    
    inventory_new = inventory_new \
        .dropDuplicates(["product_id", "date"]) \
        .filter(col("product_id").isNotNull() & col("date").isNotNull()) \
        .withColumnRenamed("stock_received", "new_stock_received") \
        .withColumnRenamed("damaged_stock", "new_damaged_stock") \
        .withColumn("date_ts", to_date(col("date")))
    
    inventory_comparison = inventory_old.alias("old") \
        .join(
            inventory_new.alias("new"),
            (col("old.product_id") == col("new.product_id")) & 
            (col("old.date_ts") == col("new.date_ts")),
            "full_outer"
        ) \
        .select(
            coalesce(col("old.product_id"), col("new.product_id")).alias("product_id"),
            coalesce(col("old.date_ts"), col("new.date_ts")).alias("date"),
            col("old.old_stock_received"),
            col("new.new_stock_received"),
            col("old.old_damaged_stock"),
            col("new.new_damaged_stock")
        )
    
    inventory_comparison = inventory_comparison \
        .withColumn("stock_variance", coalesce(col("new_stock_received"), lit(0)) - coalesce(col("old_stock_received"), lit(0))) \
        .withColumn("damaged_variance", coalesce(col("new_damaged_stock"), lit(0)) - coalesce(col("old_damaged_stock"), lit(0))) \
        .withColumn("has_discrepancy", when((col("stock_variance") != 0) | (col("damaged_variance") != 0), 1).otherwise(0)) \
        .withColumn("discrepancy_type", 
            when((col("stock_variance") != 0) & (col("damaged_variance") != 0), "Both Stock and Damaged Mismatch")
            .when(col("stock_variance") != 0, "Stock Mismatch")
            .when(col("damaged_variance") != 0, "Damaged Stock Mismatch")
            .otherwise("No Discrepancy")) \
        .withColumn("severity",
            when(abs(col("stock_variance")) > 100, "Critical")
            .when(abs(col("stock_variance")) > 50, "High")
            .when(abs(col("stock_variance")) > 10, "Medium")
            .otherwise("Low"))
    
    products_clean = products_df.dropDuplicates(["product_id"]).select(
        col("product_id").alias("prod_id"),
        col("product_name"),
        col("category"),
        col("price")
    )

    inventory_enriched = inventory_comparison \
        .join(products_clean, inventory_comparison["product_id"] == products_clean["prod_id"], "left") \
        .drop("prod_id") \
        .withColumn("financial_impact", abs(col("stock_variance")) * coalesce(col("price"), lit(0))) \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("day", dayofmonth("date"))
    
    processed_path = f"s3://{PROCESSED_BUCKET}/processed/inventory_reconciliation/"
    inventory_enriched.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(processed_path)
    
    logger.info(f"Processed data written to {processed_path}")
    job.commit()

except Exception as e:
    print(f"Error in Inventory Reconciliation ETL: {str(e)}")
    logger.error(f"Error in Inventory Reconciliation ETL: {str(e)}")
    raise