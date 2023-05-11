from kafka import KafkaProducer
from time import sleep
import json
from datetime import datetime
from json import dumps

producer=KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10,1))
producer.send('test_script',b'hellow,kafka')
producer.send('test_script',b'hellow,kafka how r u')
# By default takes json serilizer
"""
producer=KafkaProducer(
    value_serilizer=lambda m:dumps(m).encode('utf-8'),
    bootstrap_servers=['0.0.0.0:9092'])

for i in range(1,100):
    producer.send('test_script',value={'hello': i})
    sleep(0.001)
"""

# now=datetime.now()
# print(now)
# current_time=now.strftime("%d/%m/%Y %H:%M:%S")
