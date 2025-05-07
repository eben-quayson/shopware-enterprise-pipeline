import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from pyspark.sql.functions import col, when, from_unixtime, to_date, year, month, dayofmonth, hour
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

def transform_pos_data(pos_df):
    """Apply transformations to POS data."""
    return pos_df \
        .dropDuplicates(["transaction_id"]) \
        .filter((col("quantity") >= 0) & (col("revenue") >= 0)) \
        .withColumn("discount_applied", when(col("discount_applied").isNull(), 0.0).otherwise(col("discount_applied"))) \
        .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp")) \
        .withColumn("net_revenue", col("revenue") - col("discount_applied")) \
        .withColumn("date", to_date(col("timestamp"))) \
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
        )

def process_pos_data(glue_context, database_name, table_name, output_path):
    """Process POS data from source to Delta table."""
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
    
    print(f"Writing data to Delta table at: {output_path}")
    try:
        # Check if Delta table exists
        delta_table = DeltaTable.forPath(glue_context.spark_session, output_path)
        
        # Merge/upsert logic
        print("Performing Delta merge operation")
        delta_table.alias("existing").merge(
            pos_silver_df.alias("updates"),
            "existing.transaction_id = updates.transaction_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        print("Successfully merged records into the Delta table")
    except Exception as e:
        # If Delta table does not exist, create it by writing
        print(f"Delta table does not exist, creating at {output_path}")
        pos_silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .save(output_path)
        
        print("Successfully wrote records to new Delta table")

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue Delta ETL job for POS data")
    
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'database_name',
        'table_name',
        'silver_path',
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
    process_pos_data(
        glue_context,
        config["database_name"],
        config["table_name"],
        config["silver_path"]
    )
    
    print("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()