import base64
import json

import boto3


s3_client = boto3.client('s3')
BUCKET_NAME = "dbt-data-lake-378567535341"


def lambda_handler(event, context):
    for record in event['Records']:
        print(record)
        # Decode Kinesis data
        payload_str = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        print(f"Record Data: {payload_str}")
        payload = json.loads(payload_str)

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