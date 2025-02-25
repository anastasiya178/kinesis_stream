""" Create a Lambda function in AWS console with Python runtime and opy this code into the lambda """

import json
import boto3

s3_client = boto3.client('s3')
BUCKET_NAME = "your-log-bucket"


def lambda_handler(event, context):
    for record in event['Records']:
        # Decode Kinesis data
        payload = json.loads(record["kinesis"]["data"])

        # Filter logs (only store errors)
        if payload["status"] == "error":
            file_name = f'logs/{int(payload["timestamp"])}.json'

            # Save error logs to S3
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(payload)
            )

            print(f"Saved to S3: {file_name}")

    return {"statusCode": 200, "body": "Processing complete"}
