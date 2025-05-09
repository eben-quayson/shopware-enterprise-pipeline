import json
import boto3
import base64
import os
import logging

# Configuration
S3_BUCKET = os.environ.get("VALID_BUCKET")
S3_PREFIX_CUSTOMER_INTERACTIONS = "customer_interactions/"

# AWS Clients
s3_client = boto3.client('s3')

def process_customer_interaction_record(record):
    try:
        # Get the Base64-encoded data from the record
        kinesis_data = record['kinesis']['data']
        
        # Check if the data is non-empty
        if not kinesis_data:
            print("Empty data received in the record")
            return

        # Decode the Base64 data and load it as JSON
        decoded_data = base64.b64decode(kinesis_data).decode('utf-8')
        data = json.loads(decoded_data)

        # Perform validation: Check required fields for Customer Interactions
        if "customer_id" in data and "interaction_type" in data and "timestamp" in data:
            # Generate the S3 key based on 'customer_id' or timestamp
            s3_key = f"{S3_PREFIX_CUSTOMER_INTERACTIONS}{data['customer_id']}_{data['timestamp']}.json"
            
            # Write the valid data to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(data)
            )
            print(f"Valid customer interaction data written to S3 at {s3_key}")
        else:
            print(f"Invalid data (missing required fields): {data}")
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: Error processing the record data: {e}")
    except Exception as e:
        print(f"Error processing customer interaction record: {e}")

def lambda_handler(event, context):
    """
    Lambda function handler for Customer Interactions Stream.
    This function will be triggered by a Kinesis stream event.
    """
    try:
        # Iterate through each record from the Kinesis stream
        for record in event['Records']:
            process_customer_interaction_record(record)
    except Exception as e:
        print(f"Error processing event: {e}")
        raise e
