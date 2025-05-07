import sys 
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, upper, when, lit, sha2, from_unixtime, to_date, year, month, dayofmonth, hour
from delta.tables import DeltaTable
from pyspark.conf import SparkConf

def configure_spark_for_delta(spark):
    """Configure Spark session for Delta Lake integration."""
    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark.sparkContext._conf.setAll(conf.getAll())
    return spark

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

def process_crm_data(glue_context, database_name, table_name, silver_path):
    """Process CRM data from source to Delta table."""
    print(f"Reading data from {database_name}.{table_name}")
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx="datasource0"
    )
    
    crm_df = dynamic_frame.toDF()
    print(f"Source schema: {crm_df.schema}")
    print(f"Source record count: {crm_df.count()}")
    
    print("Applying transformations")
    crm_silver_df = transform_crm_data(crm_df)
    print(f"Transformed record count: {crm_silver_df.count()}")
    
    print(f"Writing data to Delta table at: {silver_path}")
    try:
        # Check if Delta table exists
        delta_table = DeltaTable.forPath(glue_context.spark_session, silver_path)
        
        # Merge/upsert logic
        print("Performing Delta merge operation")
        delta_table.alias("existing").merge(
            crm_silver_df.alias("updates"),
            "existing.customer_id = updates.customer_id AND existing.timestamp = updates.timestamp"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        print("Successfully merged records into the Delta table")
    except Exception as e:
        # If Delta table does not exist, create it
        print(f"Delta table does not exist, creating at {silver_path}")
        crm_silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("year", "month", "day") \
            .save(silver_path)
        
        print("Successfully wrote records to new Delta table")

def main():
    """Main function to execute the Glue job."""
    print("Starting AWS Glue ETL job for CRM data")
    
    # Get job parameters
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
    process_crm_data(
        glue_context,
        config["database_name"],
        config["table_name"],
        config["silver_path"]
    )
    
    print("ETL job completed successfully")
    job.commit()

if __name__ == "__main__":
    main()
