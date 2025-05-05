import json
import logging
import urllib.request
import boto3
import os

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Firehose client
firehose_client = boto3.client('firehose')

# Firehose delivery stream name from environment variables
FIREHOSE_STREAM_NAME = os.environ.get("FIREHOSE_STREAM_NAME")

# External API URL
EXTERNAL_API_URL = os.getenv("EXTERNAL_API_URL")
print(f"Fetching data from: {EXTERNAL_API_URL}")
print(f"Fetching data from: {FIREHOSE_STREAM_NAME}")
def lambda_handler(event, context):
    try:
        with urllib.request.urlopen(EXTERNAL_API_URL) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())

                # Perform validation
                if "customer_id" in data and "interaction_type" in data and "timestamp" in data:
                    firehose_client.put_record(
                        DeliveryStreamName=FIREHOSE_STREAM_NAME,
                        Record={
                            'Data': json.dumps(data) + '\n'
                        }
                    )
                    logger.info(f"Customer interaction data pushed to Firehose: {data}")
                else:
                    logger.warning(f"Invalid customer interaction data: {data}")
            else:
                logger.error(f"Failed to fetch data, status code: {response.status}")
    except Exception as e:
        logger.exception(f"Exception occurred while processing customer interaction data: {e}")
