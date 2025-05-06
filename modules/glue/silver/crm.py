import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, upper, when, lit, sha2, from_unixtime, to_date, year, month, dayofmonth, hour

def configure_spark_for_iceberg(spark, warehouse_path):
    """Configure Spark session for Iceberg integration with Glue 5.0."""
    # Set non-static Iceberg configurations
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_path)
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    
    # Additional settings for better performance with Glue 5.0
    spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    
    # Configure S3 optimizations
    spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    
    return spark

def create_or_replace_iceberg_table(spark, table_name, table_path):
    """Create or replace Iceberg table."""
    # Check if table exists and drop if it does
    spark.sql(f"""
        DROP TABLE IF EXISTS {table_name}
    """)
    
    # Create the Iceberg table with appropriate schema and partitioning
    spark.sql(f"""
        CREATE TABLE {table_name}
        (
            customer_id INT,
            interaction_type STRING,
            channel STRING,
            rating INT,
            message_excerpt STRING,
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

def transform_crm_data(crm_df):
    """Apply transformations to CRM data."""
    return crm_df \
        .dropDuplicates(["customer_id", "timestamp"]) \
        .filter(col("rating").isNull() | (col("rating").between(1, 5))) \
        .withColumn("interaction_type", upper(col("interaction_type"))) \
        .withColumn("channel", when(col("channel").isNull(), "UNKNOWN").otherwise(upper(col("channel")))) \
        .withColumn("message_excerpt", when(col("message_excerpt").isNull(), lit("")).otherwise(sha2(col("message_excerpt"), 256))) \
        .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp")) \
        .withColumn("date", to_date(col("timestamp"))) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .withColumn("hour", hour(col("timestamp"))) \
        .select(
            col("customer_id").cast(IntegerType()),
            col("interaction_type").cast(StringType()),
            col("channel").cast(StringType()),
            col("rating").cast(IntegerType()),
            col("message_excerpt").cast(StringType()),
            col("timestamp"),
            col("date"),
            col("year"),
            col("month"),
            col("day"),
            col("hour")
        )

def process_crm_data(glue_context, database_name, table_name, iceberg_table, silver_path):
    """Process CRM data from source to Iceberg table."""
    # Create DynamicFrame from Glue Data Catalog
    print(f"Reading data from {database_name}.{table_name}")
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx="datasource0"
    )
    
    # Print schema and count for debugging
    crm_df = dynamic_frame.toDF()
    print(f"Source schema: {crm_df.schema}")
    print(f"Source record count: {crm_df.count()}")
    
    # Apply transformations
    print("Applying transformations")
    cust_silver_df = transform_crm_data(crm_df)
    print(f"Transformed record count: {cust_silver_df.count()}")
    
    # Create or replace Iceberg table
    print(f"Creating Iceberg table: {iceberg_table}")
    create_or_replace_iceberg_table(glue_context.spark_session, iceberg_table, silver_path)
    
    # Write data to Iceberg table with optimized settings for Glue 5.0
    print(f"Writing data to Iceberg table: {iceberg_table}")
    cust_silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .option("fanout-enabled", "true") \
        .saveAsTable(iceberg_table)
    
    print("Data processing complete")

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue Iceberg ETL job")
    
    # Get job parameters - add all parameters you want to use
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'database_name',
        'table_name',
        'warehouse_path',
        'silver_path',
        'iceberg_table'
    ])
    
    # Initialize Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    # Configuration from job parameters
    config = {
        "database_name": args['database_name'],
        "table_name": args['table_name'],
        "warehouse_path": args['warehouse_path'],
        "silver_path": args['silver_path'],
        "iceberg_table": args['iceberg_table']
    }
    
    # Print configuration for debugging
    print(f"Using configuration: {config}")
    
    # Configure Spark for Iceberg
    print("Configuring Spark for Iceberg")
    configure_spark_for_iceberg(spark, config["warehouse_path"])
    
    # Process the data
    print(f"Processing data from {config['database_name']}.{config['table_name']}")
    process_crm_data(
        glue_context,
        config["database_name"],
        config["table_name"],
        config["iceberg_table"],
        config["silver_path"]
    )
    
    # Commit the job
    print("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()