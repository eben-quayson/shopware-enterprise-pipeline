import json
import boto3
import base64
import logging
import os
# Configuration
S3_BUCKET = os.environ.get("VALID_BUCKET")
S3_PREFIX_WEB_TRAFFIC_LOGS = "web_traffic_logs/"

# AWS Clients
s3_client = boto3.client('s3')

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_web_traffic_log_record(record):
    try:
        # Decode the Kinesis record data (Base64) and load it as JSON
        data = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))
        
        # Perform validation: Check required fields for Web Traffic Logs
        if "session_id" in data and "page" in data and "timestamp" in data:
            # Generate the S3 key based on 'session_id' or timestamp
            s3_key = f"{S3_PREFIX_WEB_TRAFFIC_LOGS}{data['session_id']}_{data['timestamp']}.json"
            
            # Write the valid data to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(data)
            )
            logger.info(f"Valid web traffic log data written to S3 at {s3_key}")
        else:
            logger.warning(f"Invalid data (missing required fields): {data}")
    except Exception as e:
        logger.error(f"Error processing web traffic log record: {e}")

def web_traffic_logs_lambda_handler(event, context):
    """
    Lambda function handler for Web Traffic Logs Stream.
    """
    try:
        for record in event['Records']:
            process_web_traffic_log_record(record)
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise e
