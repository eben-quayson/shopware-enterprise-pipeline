import boto3
import pandas as pd
import io
from datetime import datetime
from urllib.parse import urlparse

# Define S3 sources and destinations
sources = {
    "pos": "s3://external-staging-bucket-shopware/pos/",
    "inventory": "s3://external-staging-bucket-shopware/inventory/",  
}

destinations = {
    "pos": "s3://group-four-lakehouse-bucket/bronze/pos/",
    "inventory": "s3://group-four-lakehouse-bucket/bronze/inventory/",
}

s3 = boto3.client('s3')

def list_files(bucket, prefix, extension):
    """List files with a specific extension from S3 prefix"""
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith(extension):
                yield obj['Key']

def read_file_from_s3(bucket, key, file_type):
    """Read a CSV or JSON file from S3 into a DataFrame"""
    response = s3.get_object(Bucket=bucket, Key=key)
    body = io.BytesIO(response['Body'].read())
    if file_type == 'csv':
        return pd.read_csv(body)
    elif file_type == 'json':
        return pd.read_json(body, lines=True)

def write_parquet_to_s3(df, bucket, prefix):
    """Write DataFrame to S3 as a Parquet file partitioned by date (year/month/day)"""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)
    
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    
    key = f"{prefix}year={year}/month={month}/day={day}/data.parquet"
    
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"Uploaded to s3://{bucket}/{key}")


# Loop over each dataset
for dataset in sources:
    print(f"\nProcessing: {dataset}")
    
    source_url = urlparse(sources[dataset])
    dest_url = urlparse(destinations[dataset])
    
    source_bucket = source_url.netloc
    source_prefix = source_url.path.lstrip('/')
    
    dest_bucket = dest_url.netloc
    dest_prefix = dest_url.path.lstrip('/')

    file_type = 'csv' if dataset == 'pos' else 'json'
    extension = '.csv' if dataset == 'pos' else '.json'

    all_data = []

    for key in list_files(source_bucket, source_prefix, extension):
        print(f"Reading: {key}")
        df = read_file_from_s3(source_bucket, key, file_type)
        all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data)
        write_parquet_to_s3(final_df, dest_bucket, dest_prefix)
    else:
        print("No files found.")
