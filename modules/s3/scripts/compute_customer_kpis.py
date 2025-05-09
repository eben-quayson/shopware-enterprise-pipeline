import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import datetime
import traceback
from pyspark.conf import SparkConf
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])

# Initialize logger
logger = logging.getLogger('CustomerSupportKPIs')
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
input_delta_path = f"s3://{args['BUCKET_NAME']}/silver/customer_interaction/year={year}/month={month}/day={day}/"
output_gold_s3_path_base = f"s3://{args['BUCKET_NAME']}/gold/customer_support_kpis/"
logger.info(f"Input Delta Path: {input_delta_path}")
logger.info(f"Output Gold S3 Base Path: {output_gold_s3_path_base}")
logger.info(f"Processing Date for Output Partitioning: {year}-{month}-{day}")
logger.info("Configuration setup complete.")

# 1. Load Data
logger.info("Starting Data Loading phase...")
customer_interactions_df = None
try:
    logger.info(f"Attempting to read Delta table from: {input_delta_path}")
    customer_interactions_df = spark.read.format("delta").load(input_delta_path)
    logger.info("Schema of loaded customer_interactions_df:")
    customer_interactions_df.printSchema()
    record_count = customer_interactions_df.count()
    logger.info(f"Successfully read {record_count} records from Delta table.")
    if record_count == 0:
        logger.warning("Loaded DataFrame is empty. KPIs might be zero or null.")
except Exception as e:
    logger.error(f"Error reading Delta table from {input_delta_path}. Exception: {e}")
    logger.error(traceback.format_exc())
    job.commit()
    sys.exit(1)
logger.info("Data Loading phase completed.")

# 2. KPI Calculation
logger.info("Starting KPI Calculation phase...")

# KPI 1: Feedback Score (Average rating for "Feedback" interactions)
logger.info("Calculating KPI 1: Feedback Score...")
kpi_feedback_score_final_df = None
try:
    feedback_interactions_df = customer_interactions_df.filter(col("interaction_type") == "Feedback")
    feedback_count = feedback_interactions_df.count()
    logger.info(f"Found {feedback_count} interactions of type 'Feedback'.")

    feedback_score_df = feedback_interactions_df.agg(avg("rating").alias("average_feedback_score"))
    
    avg_score_row = feedback_score_df.first()

    if avg_score_row is None or avg_score_row["average_feedback_score"] is None:
        logger.warning("No feedback interactions with ratings found, or all ratings are null. Setting average_feedback_score to null.")
        avg_score_value = None
    else:
        avg_score_value = float(avg_score_row["average_feedback_score"])
        logger.info(f"Calculated average_feedback_score: {avg_score_value}")

    kpi_feedback_score_schema = StructType([
        StructField("metric_name", StringType(), False),
        StructField("value", DoubleType(), True)
    ])
    kpi_feedback_score_final_df = spark.createDataFrame(
        [("average_feedback_score", avg_score_value)],
        kpi_feedback_score_schema
    )
    logger.info("Feedback Score KPI DataFrame:")
    kpi_feedback_score_final_df.show(truncate=False)
except Exception as e:
    logger.error(f"Error calculating Feedback Score KPI: {e}")
    logger.error(traceback.format_exc())
    kpi_feedback_score_schema = StructType([
        StructField("metric_name", StringType(), False),
        StructField("value", DoubleType(), True)
    ])
    kpi_feedback_score_final_df = spark.createDataFrame([("average_feedback_score", None)], kpi_feedback_score_schema)
    logger.info("Created empty/null DataFrame for Feedback Score KPI due to error.")

# KPI 2: Interaction Volume by Type
logger.info("Calculating KPI 2: Interaction Volume by Type...")
kpi_interaction_volume_final_df = None
try:
    interaction_volume_df = customer_interactions_df.groupBy("interaction_type") \
                                                    .agg(count("*").alias("volume")) \
                                                    .orderBy("interaction_type")
    kpi_interaction_volume_final_df = interaction_volume_df
    logger.info("Interaction Volume by Type KPI DataFrame:")
    kpi_interaction_volume_final_df.show(truncate=False)
    logger.info(f"Calculated Interaction Volume for {kpi_interaction_volume_final_df.count()} types.")
except Exception as e:
    logger.error(f"Error calculating Interaction Volume KPI: {e}")
    logger.error(traceback.format_exc())
    kpi_interaction_volume_schema = StructType([
        StructField("interaction_type", StringType(), True),
        StructField("volume", LongType(), True)
    ])
    kpi_interaction_volume_final_df = spark.createDataFrame([], kpi_interaction_volume_schema)
    logger.info("Created empty DataFrame for Interaction Volume KPI due to error.")

# KPI 3: Time-to-Resolution
logger.info("Skipping Time-to-Resolution KPI: Required data (e.g., resolution timestamp) not available in the schema.")
logger.info("KPI Calculation phase completed.")

# 3. Write KPIs to S3 Gold Layer (CSV)
logger.info("Starting Data Writing phase...")

# Define output paths with date partitioning
output_path_feedback_score = f"{output_gold_s3_path_base}year={year}/month={month}/day={day}/feedback_score/"
output_path_interaction_volume = f"{output_gold_s3_path_base}year={year}/month={month}/day={day}/interaction_volume/"

# Write Feedback Score KPI
if kpi_feedback_score_final_df is not None:
    try:
        logger.info(f"Attempting to write Feedback Score KPI to: {output_path_feedback_score}")
        kpi_feedback_score_final_df.coalesce(1) \
            .write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path_feedback_score)
        logger.info(f"Feedback Score KPI written successfully to {output_path_feedback_score}")
    except Exception as e:
        logger.error(f"Error writing Feedback Score KPI to {output_path_feedback_score}: {e}")
        logger.error(traceback.format_exc())
else:
    logger.warning("Skipping write for Feedback Score KPI as its DataFrame is None (likely due to an earlier error).")

# Write Interaction Volume KPI
if kpi_interaction_volume_final_df is not None:
    try:
        logger.info(f"Attempting to write Interaction Volume KPI to: {output_path_interaction_volume}")
        kpi_interaction_volume_final_df.coalesce(1) \
            .write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path_interaction_volume)
        logger.info(f"Interaction Volume KPI written successfully to {output_path_interaction_volume}")
    except Exception as e:
        logger.error(f"Error writing Interaction Volume KPI to {output_path_interaction_volume}: {e}")
        logger.error(traceback.format_exc())
else:
    logger.warning("Skipping write for Interaction Volume KPI as its DataFrame is None (likely due to an earlier error).")

logger.info("Data Writing phase completed.")

job.commit()
logger.info(f"Job '{args['JOB_NAME']}' completed successfully.")