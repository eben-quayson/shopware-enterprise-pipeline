import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import col, when, from_unixtime, year, month, dayofmonth, hour
from delta.tables import DeltaTable
from pyspark.conf import SparkConf

def configure_spark_for_delta(spark):
    """Configure Spark session for Delta Lake integration."""
    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark.sparkContext._conf.setAll(conf.getAll())
    return spark

def transform_inventory_data(inv_df):
    """Apply transformations to inventory data."""
    columns = [c.lower() for c in inv_df.columns]
    if 'inventory_id' not in columns:
        raise ValueError("Input DataFrame is missing required column 'inventory_id'")
    
    transformed_df = inv_df \
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

    return transformed_df.select(
        col("inventory_id").cast(IntegerType()),
        col("product_id").cast(IntegerType()),
        col("warehouse_id").cast(IntegerType()),
        col("stock_level").cast(IntegerType()),
        col("restock_threshold").cast(IntegerType()),
        col("needs_restock").cast(BooleanType()),
        col("last_updated").cast(TimestampType()),
        col("year"),
        col("month"),
        col("day"),
        col("hour")
    )

def process_inventory_data(glue_context, database_name, table_name, silver_path):
    """Process inventory data and write to Delta table."""
    print(f"Attempting to read from database: {database_name}, table: {table_name}")
    try:
        dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx="datasource0"
        )
        
        inv_df = dynamic_frame.toDF()
        print(f"Source schema: {inv_df.schema}")
        
        print("Applying transformations")
        inv_silver_df = transform_inventory_data(inv_df)
        print(f"Transformed record count: {inv_silver_df.count()}")
        
        print(f"Writing data to Delta table at: {silver_path}")
        try:
            # Check if Delta table exists
            delta_table = DeltaTable.forPath(glue_context.spark_session, silver_path)
            
            # Merge/upsert logic
            delta_table.alias("existing").merge(
                inv_silver_df.alias("updates"),
                "existing.inventory_id = updates.inventory_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            
            print("Successfully merged records into the Delta table")
        except Exception as e:
            # If Delta table does not exist, create it
            print(f"Delta table does not exist, creating at {silver_path}")
            inv_silver_df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("year", "month", "day") \
                .save(silver_path)
            
            print("Successfully wrote records to new Delta table")
    
    except Exception as e:
        print(f"Error processing inventory data: {str(e)}")
        raise

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue ETL job for inventory data")
    
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
    process_inventory_data(
        glue_context,
        config["database_name"],
        config["table_name"],
        config["silver_path"]
    )
    
    print("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()
