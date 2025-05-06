import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, BooleanType
from pyspark.sql.functions import col, when, from_unixtime, to_date, year, month, dayofmonth, hour, initcap, upper, sha2

def configure_spark_for_iceberg(spark, warehouse_path):
    """Configure Spark session for Iceberg integration with Glue 5.0."""
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_path)
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    return spark

def create_or_replace_iceberg_table(spark, table_name, table_path):
    """Create or replace Iceberg table for web traffic data."""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name}
        (
            session_id STRING,
            user_id INT,
            hashed_user_id STRING,
            page STRING,
            device_type STRING,
            browser STRING,
            event_type STRING,
            timestamp TIMESTAMP,
            date DATE,
            is_anonymous BOOLEAN,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING iceberg
        PARTITIONED BY (year, month, day, hour)
        LOCATION '{table_path}'
        TBLPROPERTIES (
            'write.format.default'='parquet',
            'write.distribution-mode'='hash',
            'write.parquet.compression-codec'='snappy'
        )
    """)

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
            col("user_id").cast(IntegerType()),  # Original user_id, untouched
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

def process_web_traffic(glue_context, database_name, table_name, iceberg_table, silver_path):
    """Process web traffic data from source to Iceberg table."""
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
    
    print(f"Creating Iceberg table: {iceberg_table}")
    create_or_replace_iceberg_table(glue_context.spark_session, iceberg_table, silver_path)
    
    print(f"Writing data to Iceberg table: {iceberg_table}")
    web_silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .option("fanout-enabled", "true") \
        .saveAsTable(iceberg_table)
    
    print("Data processing complete")

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue Iceberg ETL job for web traffic data")
    
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'database_name',
        'table_name',
        'warehouse_path',
        'silver_path',
        'iceberg_table'
    ])
    
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    config = {
        "database_name": args['database_name'],
        "table_name": args['table_name'],
        "warehouse_path": args['warehouse_path'],
        "silver_path": args['silver_path'],
        "iceberg_table": args['iceberg_table']
    }
    
    print(f"Using configuration: {config}")
    
    print("Configuring Spark for Iceberg")
    configure_spark_for_iceberg(spark, config["warehouse_path"])
    
    print(f"Processing data from {config['database_name']}.{config['table_name']}")
    process_web_traffic(
        glue_context,
        config["database_name"],
        config["table_name"],
        config["iceberg_table"],
        config["silver_path"]
    )
    
    print("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()