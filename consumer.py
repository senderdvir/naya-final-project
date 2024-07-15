from kafka import KafkaConsumer
import json

# Set up Kafka consumer
consumer = KafkaConsumer(
    'polygon_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    # value_serializer=lambda v: json.loads(v.decode('utf-8'))
)

# Consume messages from the Kafka topic
for message in consumer:
    print(message.value)
