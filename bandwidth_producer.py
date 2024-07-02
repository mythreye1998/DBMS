from kafka import KafkaProducer
import json
import time

def get_message_size(message):
    return len(json.dumps(message).encode('utf-8'))

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

data = {"value": "example", "timestamp": int(time.time() * 1000)}  # Example data
message_size = get_message_size(data)  # Size in bytes
producer.send('tpch', value=data)  # Changed topic to 'tpch'

