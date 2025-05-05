import json
import logging
import urllib.request
import boto3
import os

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Firehose client
firehose_client = boto3.client('firehose')

# Environment-configurable values
FIREHOSE_STREAM_NAME = os.environ.get("FIREHOSE_STREAM_NAME")
EXTERNAL_API_URL = os.getenv("EXTERNAL_API_URL")
print(f"Fetching data from: {EXTERNAL_API_URL}")
print(f"Fetching data from: {FIREHOSE_STREAM_NAME}")
def lambda_handler(event, context):
    try:
        with urllib.request.urlopen(EXTERNAL_API_URL) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())

                # Perform validation
                if "session_id" in data and "page" in data and "timestamp" in data:
                    firehose_client.put_record(
                        DeliveryStreamName=FIREHOSE_STREAM_NAME,
                        Record={
                            'Data': json.dumps(data) + '\n'
                        }
                    )
                    logger.info(f"Web traffic log data pushed to Firehose: {data}")
                else:
                    logger.warning(f"Invalid web traffic log data: {data}")
            else:
                logger.error(f"Failed to fetch data, status code: {response.status}")
    except Exception as e:
        logger.exception(f"Error fetching web traffic log data: {e}")
