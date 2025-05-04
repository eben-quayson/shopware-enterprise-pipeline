import json
import logging
import requests
import boto3
import os


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis')

# Environment-configurable values
KINESIS_STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME")
EXTERNAL_API_URL = "http://18.203.232.58:8000/api/web-traffic/"

def lambda_handler(event, context):
    try:
        # Fetch data from the external API
        response = requests.get(EXTERNAL_API_URL)
        if response.status_code == 200:
            data = response.json()

            # Perform validation
            if "session_id" in data and "page" in data and "timestamp" in data:
                # Push valid data to Kinesis stream
                kinesis_client.put_record(
                    StreamName=KINESIS_STREAM_NAME,
                    Data=json.dumps(data),
                    PartitionKey=str(data["session_id"])
                )
                logger.info(f"Web traffic log data pushed to Kinesis: {data}")
            else:
                logger.warning(f"Invalid web traffic log data: {data}")
        else:
            logger.error(f"Failed to fetch data, status code: {response.status_code}")
    except Exception as e:
        logger.exception(f"Error fetching web traffic log data: {e}")

