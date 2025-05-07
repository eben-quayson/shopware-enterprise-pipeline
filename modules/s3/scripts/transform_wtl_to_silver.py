import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, BooleanType
from pyspark.sql.functions import col, when, from_unixtime, to_date, year, month, dayofmonth, hour, initcap, upper, sha2
from delta.tables import DeltaTable
from pyspark.conf import SparkConf

def configure_spark_for_delta(spark):
    """Configure Spark session for Delta Lake integration."""
    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    spark.sparkContext._conf.setAll(conf.getAll())
    return spark

def transform_web_traffic(web_df):
    """Apply transformations to web traffic data."""
    return web_df \
        .dropDuplicates(["session_id", "timestamp"]) \
        .withColumn("device_type", when(col("device_type").isNull(), "UNKNOWN").otherwise(initcap(col("device_type")))) \
        .withColumn("browser", when(col("browser").isNull(), "UNKNOWN").otherwise(initcap(col("browser")))) \
        .withColumn("event_type", when(col("event_type").isNull(), "UNKNOWN").otherwise(upper(col("event_type")))) \
        .withColumn("hashed_user_id", when(col("user_id").isNotNull(), sha2(col("user_id").cast("string"), 256)).otherwise(None)) \
        .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp")) \
        .withColumn("date", to_date(col("timestamp"))) \
        .withColumn("is_anonymous", col("user_id").isNull()) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .select(
            col("session_id").cast(StringType()),
            col("user_id").cast(IntegerType()),
            col("hashed_user_id").cast(StringType()),
            col("page").cast(StringType()),
            col("device_type").cast(StringType()),
            col("browser").cast(StringType()),
            col("event_type").cast(StringType()),
            col("timestamp").cast(TimestampType()),
            col("date"),
            col("is_anonymous").cast(BooleanType()),
            col("year"),
            col("month"),
            col("day"),
            col("hour")
        )

def process_web_traffic(glue_context, database_name, table_name, output_path):
    """Process web traffic data and write to Delta table."""
    print(f"Attempting to read from database: {database_name}, table: {table_name}")
    try:
        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx="datasource0"
        )
    except Exception as e:
        print(f"Error accessing catalog: {str(e)}")
        raise

    web_df = dynamic_frame.toDF()
    print(f"Source schema: {web_df.schema}")
    print(f"Source record count: {web_df.count()}")

    print("Applying transformations")
    web_silver_df = transform_web_traffic(web_df)
    print(f"Transformed record count: {web_silver_df.count()}")

    print(f"Writing data to Delta table at: {output_path}")
    try:
        delta_table = DeltaTable.forPath(glue_context.spark_session, output_path)

        print("Performing Delta merge operation")
        delta_table.alias("existing").merge(
            web_silver_df.alias("updates"),
            "existing.session_id = updates.session_id AND existing.timestamp = updates.timestamp"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print("Successfully merged records into the Delta table")
    except Exception as e:
        print(f"Delta table does not exist, creating at {output_path}")
        web_silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("year", "month", "day", "hour") \
            .save(output_path)

        print("Successfully wrote records to new Delta table")

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue Delta ETL job for web traffic data")

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'database_name',
        'table_name',
        'silver_path'
    ])

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    config = {
        "database_name": args['database_name'],
        "table_name": args['table_name'],
        "silver_path": args['silver_path']
    }

    print(f"Using configuration: {config}")
    print("Configuring Spark for Delta Lake")
    configure_spark_for_delta(spark)

    print(f"Processing data from {config['database_name']}.{config['table_name']}")
    process_web_traffic(
        glue_context,
        config["database_name"],
        config["table_name"],
        config["silver_path"]
    )

    print("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()
