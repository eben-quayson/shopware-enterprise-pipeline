import json
import logging
import requests
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

def lambda_handler(event, context):
    try:
        # Fetch data from the external API
        response = requests.get(EXTERNAL_API_URL)
        if response.status_code == 200:
            data = response.json()

            # Perform validation
            if "session_id" in data and "page" in data and "timestamp" in data:
                # Push valid data to Firehose delivery stream
                firehose_client.put_record(
                    DeliveryStreamName=FIREHOSE_STREAM_NAME,
                    Record={
                        'Data': json.dumps(data) + '\n'  # Newline for newline-delimited JSON in S3
                    }
                )
                logger.info(f"Web traffic log data pushed to Firehose: {data}")
            else:
                logger.warning(f"Invalid web traffic log data: {data}")
        else:
            logger.error(f"Failed to fetch data, status code: {response.status_code}")
    except Exception as e:
        logger.exception(f"Error fetching web traffic log data: {e}")

