from kafka import KafkaProducer
import json
import requests

api_key = '0ghkML96kmNwr0kbfaYxi6M3TOX01o3W'
url = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-01-09/2023-01-09'
params = {
    'apiKey': api_key
}

# Fetch data from the Polygon API
response = requests.get(url, params=params)
data = response.json()

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send data to Kafka topic
producer.send('polygon_data', value=data)
producer.flush()
