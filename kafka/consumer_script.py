from kafka import KafkaConsumer
consumer=KafkaConsumer(
    'test_script',
    bootstrap_servers=['0.0.0.0:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True
)
for k in consumer:
    print(k)