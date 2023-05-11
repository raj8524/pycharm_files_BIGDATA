import boto3
import json
s3_client=boto3.client(service_name='s3',
    region_name='ap-south-1',
    aws_access_key_id='AKIAQFOVFI5OV7QZW6EF',
    aws_secret_access_key='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')
source_bucket_name='test-sns-lambda'
file='ram'
theobjects = s3_client.list_objects_v2(Bucket=source_bucket_name, StartAfter=file )
for object in theobjects['Contents']:
    file_path=object['Key']
    print(file_path)