import boto3
import json

def lambda_handler(event, context):
    sfn_client = boto3.client('stepfunctions')
    arn = event.get('stepFunctionArn')

    if not arn:
        raise ValueError("Missing Step Function ARN")

    response = sfn_client.start_execution(
        stateMachineArn=arn,
        input=json.dumps({"triggeredBy": "EventBridge"})
    )
    return response
