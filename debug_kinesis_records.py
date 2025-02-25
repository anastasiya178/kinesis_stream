"""
You can use this boto3 functions to get iterators and responses (= data logs sent to Kinesis before
they get processed by lambda
"""


import boto3
import json

client = boto3.client("kinesis", region_name="us-east-1")

# for CLI
# aws kinesis get-shard-iterator \
# >     --stream-name log_stream \
# >     --shard-id shardId-000000000002 \
# >     --shard-iterator-type TRIM_HORIZON

iterator = client.get_shard_iterator(
    StreamName='log_stream',
    ShardId='shardId-000000000002',
    ShardIteratorType='TRIM_HORIZON',
)


response = client.get_records(
    ShardIterator=iterator['ShardIterator'],
    Limit=123,
)

print(response)