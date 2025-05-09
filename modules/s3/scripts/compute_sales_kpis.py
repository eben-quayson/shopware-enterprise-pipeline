import sys
import boto3
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as _sum, max as _max, when, lit, first as _first
from pyspark.sql.types import DateType
from pyspark.conf import SparkConf
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting Glue job script")

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Glue context initialized")

# Configure Spark for Delta Lake
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.sparkContext._conf.setAll(conf.getAll())

logger.info("Spark configured for Delta Lake")

# Date setup
processing_date = datetime.now().strftime("%Y-%m-%d")
year, month, day = processing_date.split("-")
BUCKET_NAME = args['BUCKET_NAME']

# Paths
pos_s3_path = f"s3://{BUCKET_NAME}/silver/pos/year={year}/month={month}/day={day}/"
inventory_s3_path = f"s3://{BUCKET_NAME}/silver/inventory/year={year}/month={month}/day={day}/"
s3_output_path = f"s3://{BUCKET_NAME}/gold/sales_mart/year={year}/month={month}/day={day}/"

logger.info(f"Paths set: POS data path: {pos_s3_path}, Inventory data path: {inventory_s3_path}, Output path: {s3_output_path}")

logger.info(f"Processing date: {processing_date}")

# Read POS data
logger.info("Reading POS data")
pos_df = spark.read.format("delta").load(pos_s3_path) \
    .filter((col("year") == year) & (col("month") == month) & (col("day") == "08"))
logger.info(f"POS data read successfully with {pos_df.count()} records")

# Read Inventory data
logger.info("Reading Inventory data")
inventory_df = spark.read.format("delta").load(inventory_s3_path) \
    .filter((col("year") == year) & (col("month") == month) & (col("day") == "07")) \
    .groupBy("product_id") \
    .agg(
        _max("stock_level").alias("stock_availability"),
        _max("restock_threshold").alias("restock_threshold")
    )
logger.info(f"Inventory data read successfully with {inventory_df.count()} records")

# Aggregate POS data
logger.info("Aggregating POS data")
sales_df = pos_df.groupBy( "store_id") \
    .agg(
        _sum("revenue").alias("total_sales"),
        _sum("quantity").alias("total_units"),
        _first("product_id").alias("product_id"),
    )
logger.info(f"POS data aggregated successfully with {sales_df.count()} records")

# Join sales and inventory
logger.info("Joining sales and inventory data")
combined_df = sales_df.join(inventory_df, "product_id", "inner") \
    .select(
        sales_df.product_id,
        sales_df.store_id,
        sales_df.total_sales,
        sales_df.total_units,
        inventory_df.stock_availability,
        when(col("stock_availability") < col("restock_threshold"), True).otherwise(False).alias("below_restock_threshold")
    )

combined_df = combined_df.dropDuplicates(["product_id", "store_id"])
logger.info(f"Sales and inventory data joined successfully with {combined_df.count()} records")

# Add turnover rate and date
logger.info("Adding turnover rate and processing date")
final_df = combined_df.withColumn(
    "product_turnover_rate",
    when(col("stock_availability") > 0, col("total_units") / col("stock_availability")).otherwise(0.0)
).withColumn("date", lit(processing_date).cast(DateType()))
logger.info("Turnover rate and processing date added")

# Write to S3 as CSV
logger.info(f"Writing final data to S3 at {s3_output_path}")
final_df.write.mode("overwrite").csv(s3_output_path, header=True)
logger.info("Final data written to S3 successfully")

# Commit the Glue job
logger.info("Committing Glue job")
job.commit()
logger.info("Glue job committed successfully")
