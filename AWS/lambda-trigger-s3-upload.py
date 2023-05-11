import json
import boto3
import urllib
def lambda_handler(event,context):
    s3_client=boto3.client('s3')
    bucket_name=event['Records'][0]['s3']['bucket']['name']
    key=event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote_plus(key,encoding='utf-8')
    message='file got uploaded' + key + 'to this bucket ' + bucket_name
    print(message)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        # Deserialize the files's content
        content = response['Body'].read().decode()
        data = json.loads(content)
        print(data)

    except  Exception as e:
        print(e)
        raise e


