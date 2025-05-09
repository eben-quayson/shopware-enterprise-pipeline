import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, to_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import datetime
import traceback
import logging
from pyspark.conf import SparkConf

## @params: [JOB_NAME, BUCKET_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])

# Initialize logger
logger = logging.getLogger('OperationsKPIs')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize SparkContext, GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configure Spark for Delta Lake
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.sparkContext._conf.setAll(conf.getAll())

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger.info(f"Job '{args['JOB_NAME']}' initialized.")

# Configuration
logger.info("Setting up job configurations...")
processing_date = datetime.date.today()
year, month, day = processing_date.strftime("%Y"), processing_date.strftime("%m"), processing_date.strftime("%d")
bucket_name = args['BUCKET_NAME']
input_pos_path = f"s3://{bucket_name}/silver/pos/year={year}/month={month}/day={day}/"
input_inventory_path = f"s3://{bucket_name}/silver/inventory/year={year}/month={month}/day={day}/"
output_gold_s3_path_base = f"s3://{bucket_name}/gold/operations_kpis/"
logger.info(f"Input POS Delta Path: {input_pos_path}")
logger.info(f"Input Inventory Delta Path: {input_inventory_path}")
logger.info(f"Output Gold S3 Base Path: {output_gold_s3_path_base}")
logger.info(f"Processing Date for Output Partitioning: {year}-{month}-{day}")
logger.info("Configuration setup complete.")

# 1. Load Data
logger.info("Starting Data Loading phase...")
pos_df = None
inventory_df = None
try:
    logger.info(f"Attempting to read POS Delta table from: {input_pos_path}")
    pos_df = spark.read.format("delta").load(input_pos_path)
    logger.info("Schema of loaded pos_df:")
    pos_df.printSchema()
    pos_record_count = pos_df.count()
    logger.info(f"Successfully read {pos_record_count} records from POS Delta table.")
    if pos_record_count == 0:
        logger.warning("POS DataFrame is empty. KPIs might be zero or null.")

    logger.info(f"Attempting to read Inventory Delta table from: {input_inventory_path}")
    inventory_df = spark.read.format("delta").load(input_inventory_path)
    logger.info("Schema of loaded inventory_df:")
    inventory_df.printSchema()
    inventory_record_count = inventory_df.count()
    logger.info(f"Successfully read {inventory_record_count} records from Inventory Delta table.")
    if inventory_record_count == 0:
        logger.warning("Inventory DataFrame is empty. KPIs might be zero or null.")
except Exception as e:
    logger.error(f"Error reading Delta tables. Exception: {e}")
    logger.error(traceback.format_exc())
    job.commit()
    sys.exit(1)
logger.info("Data Loading phase completed.")

# 2. KPI Calculation
logger.info("Starting KPI Calculation phase...")

# Convert epoch timestamps to date for filtering
pos_df = pos_df.withColumn("transaction_date", to_date(from_unixtime(col("timestamp"))))
inventory_df = inventory_df.withColumn("update_date", to_date(from_unixtime(col("last_updated"))))

# Filter for the specific processing date
processing_date_str = f"{year}-{month}-{day}"
pos_df = pos_df.filter(col("transaction_date") == processing_date_str)
inventory_df = inventory_df.filter(col("update_date") == processing_date_str)

# KPI 1: Inventory Turnover
logger.info("Calculating KPI 1: Inventory Turnover...")
kpi_inventory_turnover_df = None
try:
    # Total quantity sold per product from POS data
    quantity_sold_df = pos_df.groupBy("product_id").agg(sum("quantity").alias("total_quantity_sold"))

    # Average inventory level per product from inventory data
    avg_inventory_df = inventory_df.groupBy("product_id").agg(avg("stock_level").alias("avg_inventory_level"))

    # Join and calculate turnover
    turnover_df = quantity_sold_df.join(avg_inventory_df, "product_id", "left") \
                                 .select(
                                     col("product_id"),
                                     when(col("avg_inventory_level") > 0,
                                          col("total_quantity_sold") / col("avg_inventory_level"))
                                     .otherwise(None).alias("inventory_turnover")
                                 )

    kpi_inventory_turnover_df = turnover_df
    logger.info("Inventory Turnover KPI DataFrame:")
    kpi_inventory_turnover_df.show(truncate=False)
    logger.info(f"Calculated Inventory Turnover for {kpi_inventory_turnover_df.count()} products.")
except Exception as e:
    logger.error(f"Error calculating Inventory Turnover KPI: {e}")
    logger.error(traceback.format_exc())
    kpi_inventory_turnover_schema = StructType([
        StructField("product_id", LongType(), False),
        StructField("inventory_turnover", DoubleType(), True)
    ])
    kpi_inventory_turnover_df = spark.createDataFrame([], kpi_inventory_turnover_schema)
    logger.info("Created empty DataFrame for Inventory Turnover KPI due to error.")

# KPI 2: Restock Frequency
logger.info("Calculating KPI 2: Restock Frequency...")
kpi_restock_frequency_df = None
try:
    # Count instances where stock_level <= restock_threshold
    restock_events_df = inventory_df.filter(col("stock_level") <= col("restock_threshold")) \
                                   .groupBy("product_id", "warehouse_id") \
                                   .agg(count("*").alias("restock_frequency"))

    kpi_restock_frequency_df = restock_events_df
    logger.info("Restock Frequency KPI DataFrame:")
    kpi_restock_frequency_df.show(truncate=False)
    logger.info(f"Calculated Restock Frequency for {kpi_restock_frequency_df.count()} product-warehouse combinations.")
except Exception as e:
    logger.error(f"Error calculating Restock Frequency KPI: {e}")
    logger.error(traceback.format_exc())
    kpi_restock_frequency_schema = StructType([
        StructField("product_id", LongType(), False),
        StructField("warehouse_id", LongType(), False),
        StructField("restock_frequency", LongType(), True)
    ])
    kpi_restock_frequency_df = spark.createDataFrame([], kpi_restock_frequency_schema)
    logger.info("Created empty DataFrame for Restock Frequency KPI due to error.")

# KPI 3: Stockout Alerts
logger.info("Calculating KPI 3: Stockout Alerts...")
kpi_stockout_alerts_df = None
try:
    # Identify products with stock_level <= 0
    stockout_df = inventory_df.filter(col("stock_level") <= 0) \
                             .groupBy("product_id", "warehouse_id") \
                             .agg(count("*").alias("stockout_count"))

    kpi_stockout_alerts_df = stockout_df
    logger.info("Stockout Alerts KPI DataFrame:")
    kpi_stockout_alerts_df.show(truncate=False)
    logger.info(f"Calculated Stockout Alerts for {kpi_stockout_alerts_df.count()} product-warehouse combinations.")
except Exception as e:
    logger.error(f"Error calculating Stockout Alerts KPI: {e}")
    logger.error(traceback.format_exc())
    kpi_stockout_alerts_schema = StructType([
        StructField("product_id", LongType(), False),
        StructField("warehouse_id", LongType(), False),
        StructField("stockout_count", LongType(), True)
    ])
    kpi_stockout_alerts_df = spark.createDataFrame([], kpi_stockout_alerts_schema)
    logger.info("Created empty DataFrame for Stockout Alerts KPI due to error.")

logger.info("KPI Calculation phase completed.")

# 3. Write KPIs to S3 Gold Layer (CSV)
logger.info("Starting Data Writing phase...")

# Define output paths with date partitioning
output_path_inventory_turnover = f"{output_gold_s3_path_base}year={year}/month={month}/day={day}/inventory_turnover/"
output_path_restock_frequency = f"{output_gold_s3_path_base}year={year}/month={month}/day={day}/restock_frequency/"
output_path_stockout_alerts = f"{output_gold_s3_path_base}year={year}/month={month}/day={day}/stockout_alerts/"

# Write Inventory Turnover KPI
if kpi_inventory_turnover_df is not None:
    try:
        logger.info(f"Attempting to write Inventory Turnover KPI to: {output_path_inventory_turnover}")
        kpi_inventory_turnover_df.coalesce(1) \
            .write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path_inventory_turnover)
        logger.info(f"Inventory Turnover KPI written successfully to {output_path_inventory_turnover}")
    except Exception as e:
        logger.error(f"Error writing Inventory Turnover KPI to {output_path_inventory_turnover}: {e}")
        logger.error(traceback.format_exc())
else:
    logger.warning("Skipping write for Inventory Turnover KPI as its DataFrame is None.")

# Write Restock Frequency KPI
if kpi_restock_frequency_df is not None:
    try:
        logger.info(f"Attempting to write Restock Frequency KPI to: {output_path_restock_frequency}")
        kpi_restock_frequency_df.coalesce(1) \
            .write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path_restock_frequency)
        logger.info(f"Restock Frequency KPI written successfully to {output_path_restock_frequency}")
    except Exception as e:
        logger.error(f"Error writing Restock Frequency KPI to {output_path_restock_frequency}: {e}")
        logger.error(traceback.format_exc())
else:
    logger.warning("Skipping write for Restock Frequency KPI as its DataFrame is None.")

# Write Stockout Alerts KPI
if kpi_stockout_alerts_df is not None:
    try:
        logger.info(f"Attempting to write Stockout Alerts KPI to: {output_path_stockout_alerts}")
        kpi_stockout_alerts_df.coalesce(1) \
            .write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path_stockout_alerts)
        logger.info(f"Stockout Alerts KPI written successfully to {output_path_stockout_alerts}")
    except Exception as e:
        logger.error(f"Error writing Stockout Alerts KPI to {output_path_stockout_alerts}: {e}")
        logger.error(traceback.format_exc())
else:
    logger.warning("Skipping write for Stockout Alerts KPI as its DataFrame is None.")

logger.info("Data Writing phase completed.")

job.commit()
logger.info(f"Job '{args['JOB_NAME']}' completed successfully.")