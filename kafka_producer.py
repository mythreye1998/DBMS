import os
from confluent_kafka import Producer

# Kafka producer configuration
p = Producer({
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.kbytes': 10240000
})

# Directory where the .csv files are located
data_directory = '/home/voohitha/Downloads/TPC-H-V3.0.1/dbgen'

# List of TPC-H data files to publish
tpch_data_files = [
    'supplier.csv',
    'region.csv',
    'part.csv',
    'partsupp.csv',
    'orders.csv',
    'nation.csv',
    'lineitem.csv',
    'customer.csv'
]

# Specify the Kafka topic
kafka_topic = 'tpch'

# Iterate through TPC-H data files and send them to the Kafka topic
for data_file in tpch_data_files:
    file_path = os.path.join(data_directory, data_file)
    with open(file_path, 'r') as file:
        for line in file:
            # Produce message to Kafka topic
            p.produce(kafka_topic, key=None, value=line)
            # Regularly call poll to process delivery reports
            p.poll(0)
            
    # It's a good practice to call flush after finishing with each file
    p.flush()

# Final flush to make sure all messages are delivered
p.flush()
