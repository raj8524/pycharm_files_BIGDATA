#  python 3.8, pandas 1.2.4
#https://www.gormanalysis.com/blog/connecting-to-aws-s3-with-python/
import boto3
from botocore.client import ClientError
s3 = boto3.resource(
    service_name='s3',
    region_name='ap-south-1',
    aws_access_key_id='AKIA3SZJILMEM3H3KFHX',
    aws_secret_access_key='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')
# for bucket in s3.buckets.all():
#     print(bucket.name)


import pandas as pd
# Make dataframes
foo = pd.DataFrame({'x': [1, 2, 3], 'y': ['a', 'b', 'c']})
bar = pd.DataFrame({'x': [10, 20, 30], 'y': ['aa', 'bb', 'cc']})

# Save to csv
foo.to_csv('foo.csv')
bar.to_csv('bar.csv')
s3.Bucket('python-s3-bucks2').upload_file(Filename='foo.csv', Key='foo.csv')
s3.Bucket('python-s3-bucks2').upload_file(Filename='bar.csv', Key='bar.csv')
s3.Bucket('python-s3-bucks2').upload_file(Filename='D:\Study\PYTEST.txt',Key='PYTEST.txt')

for obj in s3.Bucket('kraj-88').objects.all():
    print(obj)

obj = s3.Bucket('kraj-88').Object('foo.csv').get()
foo = pd.read_csv(obj['Body'], index_col=0)
print(foo)

#--------------deleting bucket----
# for key in bucket.objects.all():
#     key.delete()
# bucket.delete()

#--------------iteration of buckets and keys--------
for bucket in s3.buckets.all():
    for key in bucket.objects.all():
        print(key.key)