import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import col, when, from_unixtime, year, month, dayofmonth, hour

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
    """Create or replace Iceberg table for inventory data."""
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"""
        CREATE TABLE {table_name}
        (
            inventory_id INT,
            product_id INT,
            warehouse_id INT,
            stock_level INT,
            restock_threshold INT,
            needs_restock BOOLEAN,
            last_updated TIMESTAMP,
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

def transform_inventory_data(inv_df):
    """Apply transformations to inventory data."""
    # First, verify the incoming schema
    print("Input DataFrame schema:")
    inv_df.printSchema()
    
    # Handle case sensitivity in column names
    columns = [c.lower() for c in inv_df.columns]
    if 'inventory_id' not in columns:
        raise ValueError("Input DataFrame is missing required column 'inventory_id'")
    
    # Apply transformations
    transformed_df = inv_df \
        .withColumnRenamed("inventory_id", "inventory_id") \
        .withColumnRenamed("product_id", "product_id") \
        .withColumnRenamed("warehouse_id", "warehouse_id") \
        .withColumnRenamed("stock_level", "stock_level") \
        .withColumnRenamed("restock_threshold", "restock_threshold") \
        .withColumnRenamed("last_updated", "last_updated") \
        .dropDuplicates(["inventory_id"]) \
        .filter(col("stock_level") >= 0) \
        .withColumn("restock_threshold", 
                   when(col("restock_threshold").isNull(), 0)
                   .otherwise(col("restock_threshold"))) \
        .filter(col("restock_threshold") >= 0) \
        .withColumn("last_updated", from_unixtime(col("last_updated")).cast("timestamp")) \
        .withColumn("needs_restock", col("stock_level") <= col("restock_threshold")) \
        .withColumn("year", year(col("last_updated"))) \
        .withColumn("month", month(col("last_updated"))) \
        .withColumn("day", dayofmonth(col("last_updated"))) \
        .withColumn("hour", hour(col("last_updated")))
    
    # Select final columns with explicit casting
    return transformed_df.select(
        col("inventory_id").cast(IntegerType()).alias("inventory_id"),
        col("product_id").cast(IntegerType()).alias("product_id"),
        col("warehouse_id").cast(IntegerType()).alias("warehouse_id"),
        col("stock_level").cast(IntegerType()).alias("stock_level"),
        col("restock_threshold").cast(IntegerType()).alias("restock_threshold"),
        col("needs_restock").cast(BooleanType()).alias("needs_restock"),
        col("last_updated").cast(TimestampType()).alias("last_updated"),
        col("year"),
        col("month"),
        col("day"),
        col("hour")
    )

def process_inventory_data(glue_context, database_name, table_name, iceberg_table, silver_path):
    """Process inventory data from source to Iceberg table."""
    print(f"Attempting to read from database: {database_name}, table: {table_name}")
    try:
        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx="datasource0"
        )
        
        # Convert to DataFrame and verify
        inv_df = dynamic_frame.toDF()
        print("Source DataFrame schema before transformations:")
        inv_df.printSchema()
        
        if inv_df.count() == 0:
            print("Warning: Source DataFrame is empty")
        
        print("Applying transformations")
        inv_silver_df = transform_inventory_data(inv_df)
        print("Transformed DataFrame schema:")
        inv_silver_df.printSchema()
        print(f"Transformed record count: {inv_silver_df.count()}")
        
        print(f"Creating Iceberg table: {iceberg_table}")
        create_or_replace_iceberg_table(glue_context.spark_session, iceberg_table, silver_path)
        
        print(f"Writing data to Iceberg table: {iceberg_table}")
        inv_silver_df.write \
            .format("iceberg") \
            .mode("append") \
            .option("fanout-enabled", "true") \
            .saveAsTable(iceberg_table)
        
        print("Data processing complete")
        
    except Exception as e:
        print(f"Error processing inventory data: {str(e)}")
        raise

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue Iceberg ETL job for inventory data")
    
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
    process_inventory_data(
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