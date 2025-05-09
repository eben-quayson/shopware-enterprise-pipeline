import boto3
import datetime
import urllib.parse
import sys
from awsglue.utils import getResolvedOptions

# Get job parameters (passed as --KEY value in Glue)
args = getResolvedOptions(sys.argv, [
    'SOURCE_BUCKET',
    'SOURCE_PREFIX',
    'DESTINATION_BUCKET',
    'DESTINATION_PREFIX'
])

# Extract parameters
source_bucket = args['SOURCE_BUCKET']
source_prefix = args['SOURCE_PREFIX']
destination_bucket = args['DESTINATION_BUCKET']
destination_prefix = args['DESTINATION_PREFIX']

# Initialize S3 client
s3_client = boto3.client('s3')

def main():
    # Get current timestamp for partitioning
    now = datetime.datetime.now(datetime.timezone.utc)
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    hour = now.strftime('%H')

    # Construct destination prefix with partitions
    destination_path = f"{destination_prefix}year={year}/month={month}/day={day}/hour={hour}/"

    try:
        # List objects in source bucket
        response = s3_client.list_objects_v2(
            Bucket=source_bucket,
            Prefix=source_prefix
        )

        if 'Contents' not in response:
            print("No objects found in source prefix.")
            return

        # Process each object
        for obj in response['Contents']:
            source_key = obj['Key']
            if source_key.endswith('/'):
                continue  # skip folders

            file_name = source_key.split('/')[-1]
            destination_key = f"{destination_path}{file_name}"

            # Copy object
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3_client.copy_object(
                Bucket=destination_bucket,
                Key=destination_key,
                CopySource=copy_source
            )
            print(f"Copied {source_key} to {destination_key}")

    except Exception as e:
        print(f"Error processing files: {str(e)}")
        raise

if __name__ == "__main__":
    main()
