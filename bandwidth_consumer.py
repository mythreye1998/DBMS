from confluent_kafka import Consumer, KafkaError
import time

# Define your Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'tpch_group',
    'auto.offset.reset': 'earliest',  # You can adjust this based on your requirements
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic you want to monitor
topic = 'tpch'
consumer.subscribe([topic])

# Initialize variables
total_bytes_consumed = 0
start_time = time.time()

try:
    while True:
        msg = consumer.poll(1.0)  # Adjust the polling interval as needed

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Reached end of partition for topic {msg.topic()}, partition {msg.partition()}')
            else:
                print(f'Error while consuming from topic {msg.topic()}: {msg.error()}')
        else:
            total_bytes_consumed += len(msg.value())

        # Calculate elapsed time
        elapsed_time = time.time() - start_time

        # Calculate average bandwidth (bytes per second)
        average_bandwidth = total_bytes_consumed / elapsed_time

        print(f'Average Bandwidth: {average_bandwidth} bytes/second')

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

