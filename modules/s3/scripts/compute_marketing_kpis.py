import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg as _avg, count as _count, when, lit
from pyspark.sql.types import DateType
from pyspark.conf import SparkConf
from datetime import datetime

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark for Delta Lake
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.sparkContext._conf.setAll(conf.getAll())


# Date setup
processing_date = datetime.now().strftime("%Y-%m-%d")
year, month, day = processing_date.split("-")

# Paths
web_traffic_s3_path = f"s3://{args['BUCKET_NAME']}/silver/wtl/year={year}/month={month}/day={day}/"
crm_s3_path = f"s3://{args['BUCKET_NAME']}/silver/customer_interaction/year={year}/month={month}/day={day}/"
s3_output_path = f"s3://{args['BUCKET_NAME']}/gold/marketing_mart/year={year}/month={month}/day={day}/"

# Read Web Traffic Logs data
web_traffic_df = spark.read.format("delta").load(web_traffic_s3_path) \
    .filter((col("year") == year) & (col("month") == month))

# Read CRM Interactions data
crm_df = spark.read.format("delta").load(crm_s3_path) \
    .filter((col("year") == year) & (col("month") == month))

# Aggregate Web Traffic data for Session Duration and Bounce Rate
web_agg_df = web_traffic_df.groupBy("session_id", "user_id") \
    .agg(
        _avg(when(col("event_type") == "page_view", lit(1)).otherwise(lit(0)).cast("int")).alias("page_views"),
        _avg(when(col("event_type") == "bounce", lit(1)).otherwise(lit(0)).cast("int")).alias("bounces")
    )

web_agg_df = web_agg_df \
    .withColumn("avg_session_duration", lit(0.0)) \
    .withColumn("bounce_rate", when(col("page_views") > 0, col("bounces") / col("page_views")).otherwise(0.0)) \
    .na.fill({"bounce_rate": 0.0})

# Aggregate CRM data
crm_agg_df = crm_df.groupBy("customer_id") \
    .agg(
        _avg(when(col("rating").isNotNull(), col("rating")).otherwise(lit(0))).alias("engagement_score"),
        _count(when(col("interaction_type") == "loyalty_action", True)).alias("loyalty_actions")
    )

# Join Web and CRM data
combined_df = web_agg_df.join(crm_agg_df, web_agg_df.user_id == crm_agg_df.customer_id, "left") \
    .select(
        crm_agg_df.customer_id,
        crm_agg_df.engagement_score,
        web_agg_df.avg_session_duration,
        web_agg_df.bounce_rate,
        lit(0.0).alias("loyalty_activity_rate"),
        crm_agg_df.loyalty_actions
    )

# Compute loyalty activity rate
total_interactions = crm_df.groupBy("customer_id").agg(_count(lit(1)).alias("total_interactions"))

loyalty_rate_df = combined_df.join(total_interactions, "customer_id", "left") \
    .withColumn(
        "loyalty_activity_rate",
        when(col("total_interactions") > 0, col("loyalty_actions") / col("total_interactions")).otherwise(0.0)
    ).drop("total_interactions")

# Clean and finalize
final_df = loyalty_rate_df.dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("date", lit(processing_date).cast("date")) \
    .na.fill({
        "engagement_score": 0.0,
        "avg_session_duration": 0.0,
        "bounce_rate": 0.0,
        "loyalty_activity_rate": 0.0,
        "loyalty_actions": 0
    })

# Write to S3
final_df.write.mode("overwrite").csv(s3_output_path, header=True)

# Commit job
job.commit()