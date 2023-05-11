import boto3
import json
import time
client=boto3.client('athena')
def lambda_handler(event,context):
    response=client.start_query_execution(
        QueryString='SELECT * FROM "glue_customer"."input" limit 10;',
        QueryExecutionContext={
            'Database':'long_region' },

        ResultConfiguration={
            'OutputLocation':'s3://emr-demo-2/output/'
        }
    )

    queryExecutionId=response['QueryExecutionId']
    response2=client.get_query_execution(QueryExecutionId=queryExecutionId)
    print(response2['QueryExecution']['Status']['State'])
    time.sleep(20)

    response2 = client.get_query_execution(QueryExecutionId=queryExecutionId)
    print(response2['QueryExecution']['Status']['State'])

    results=client.get_query_results(QueryExecutionId=queryExecutionId)
    for row in results['ResultSet']['Rows']:
        print(row)