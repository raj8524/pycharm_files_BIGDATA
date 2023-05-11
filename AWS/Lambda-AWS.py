# s3bucket-lambda
import json
import boto3
s3=boto3.resource("s3")
def lambda_handler(event,context):
    bucket_list=[]
    for bucket in s3.buckets.all():
        print(bucket.name)
        bucket_list.append(bucket.name)
        return {
            'statusCode':200,
            'body':bucket_list
        }

#lambda-dynamodb
import json
import boto3
dynamodb=boto3.resource("dynamodb")
table=dynamodb.Table('planets')
def lambda_handler(event,context):
    response=table.get_item(
        key={
            "id":"mars"
        }
    )
    print(response)
    return {
        "statusCode":200,
        "body":response

        }
