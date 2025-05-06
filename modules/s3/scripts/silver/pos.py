import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.functions import col, when, from_unixtime, to_date, year, month, dayofmonth, hour

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
    """Create or replace Iceberg table for POS data."""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name}
        (
            transaction_id STRING,
            store_id INT,
            product_id INT,
            quantity INT,
            revenue FLOAT,
            discount_applied FLOAT,
            net_revenue FLOAT,
            timestamp TIMESTAMP,
            date DATE,
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

def transform_pos_data(pos_df):
    """Apply transformations to POS data."""
    return pos_df \
        .dropDuplicates(["transaction_id"]) \
        .filter((col("quantity") >= 0) & (col("revenue") >= 0)) \
        .withColumn("discount_applied", when(col("discount_applied").isNull(), 0.0).otherwise(col("discount_applied"))) \
        .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp")) \
        .withColumn("net_revenue", col("revenue") - col("discount_applied")) \
        .withColumn("date", to_date(col("timestamp"))) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .select(
            col("transaction_id").cast(StringType()),
            col("store_id").cast(IntegerType()),
            col("product_id").cast(IntegerType()),
            col("quantity").cast(IntegerType()),
            col("revenue").cast(FloatType()),
            col("discount_applied").cast(FloatType()),
            col("net_revenue").cast(FloatType()),
            col("timestamp").cast(TimestampType()),
            col("date"),
            col("year"),
            col("month"),
            col("day"),
            col("hour")
        )


def process_pos_data(glue_context, database_name, table_name, iceberg_table, silver_path):
    """Process POS data from source to Iceberg table."""
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
    
    pos_df = dynamic_frame.toDF()
    print(f"Source schema: {pos_df.schema}")
    print(f"Source record count: {pos_df.count()}")
    
    print("Applying transformations")
    pos_silver_df = transform_pos_data(pos_df)
    print(f"Transformed record count: {pos_silver_df.count()}")
    
    print(f"Creating Iceberg table: {iceberg_table}")
    create_or_replace_iceberg_table(glue_context.spark_session, iceberg_table, silver_path)
    
    print(f"Writing data to Iceberg table: {iceberg_table}")
    pos_silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .option("fanout-enabled", "true") \
        .saveAsTable(iceberg_table)
    
    print("Data processing complete")

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue Iceberg ETL job for POS data")
    
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
    process_pos_data(
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