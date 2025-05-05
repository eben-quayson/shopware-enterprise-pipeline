import json
import logging
import requests
import boto3
import os
import base64

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Firehose client
firehose_client = boto3.client('firehose')

# Firehose delivery stream name from environment variables
FIREHOSE_STREAM_NAME = os.environ.get("FIREHOSE_STREAM_NAME")

# External API URL (Replace with the actual URL)
EXTERNAL_API_URL = os.getenv("EXTERNAL_API_URL")

def lambda_handler(event, context):
    try:
        # Fetch data from the external API
        response = requests.get(EXTERNAL_API_URL)
        if response.status_code == 200:
            data = response.json()

            # Perform validation
            if "customer_id" in data and "interaction_type" in data and "timestamp" in data:
                # Push valid data to Firehose delivery stream
                firehose_client.put_record(
                    DeliveryStreamName=FIREHOSE_STREAM_NAME,
                    Record={
                        'Data': json.dumps(data) + '\n'  # Add newline for S3-friendly format
                    }
                )
                logger.info(f"Customer interaction data pushed to Firehose: {data}")
            else:
                logger.warning(f"Invalid customer interaction data: {data}")
        else:
            logger.error(f"Failed to fetch data, status code: {response.status_code}")
    except Exception as e:
        logger.exception(f"Exception occurred while processing customer interaction data: {e}")

