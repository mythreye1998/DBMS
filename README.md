Streaming Data for Traditional Database Workload

The project focuses on implementing a streaming setup for traditional database using TPC-H benchmark. 
Streaming Platform : Apache kafka
Database : Postgres
Dataset : TPC-H

Our project is implemented in different phases :
 Setting Up Your Environment:
Install PostgreSQL: Ensure you have a working installation of PostgreSQL.
Set Up a Streaming Platform: Choose and set up a streaming platform (Apache Kafka ) that will feed data into PostgreSQL.
Commands :
—Intiate Zookeeper server :/Downloads/kafka_2.12-3.6.0$ bin/zookeeper-server-start.sh config/zookeeper.properties
—Kafka serve : ~/Downloads/kafka_2.12-3.6.0$ bin/kafka-server-start.sh config/server.properties
—CREATE KAFKA TOPIC : /Downloads/kafka_2.12-3.6.0/confluent-7.5.2/bin/sh kafka-topics --create --topic tpch --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
—LIST TOPIC : /Downloads/kafka_2.12-3.6.0/confluent-7.5.2/bin$ sh kafka-topics --list --bootstrap-server localhost:9092
—Schema Registry property : ~/Downloads/kafka_2.12-3.6.0/confluent-7.5.2$ bin/schema-registry-start etc/schema-registry/schema-registry.properties ( edit the properties )
—CREATED A "CONFIG file" UNDER confluent-7.5.2 which has connect-avro-standalone_postgres.properties and sink-quickstart-Postgres.properties.
—UPDATE PLUGIN.PATH IN : home/voohitha/Downloads/kafka_2.12-3.6.0/confluent-7.5.2/config/connect-avro-standalone_postgres.properties as plugin.path=share/java,/home/voohitha/Downloads/kafka_2.12-3.6.0/confluent-7.5.2/share/java/kafka-connect-jdbc,/home/voohitha/Downloads/kafka_2.12-3.6.0/confluent-7.5.2/share/java ( for connect-avro-standalone_postgres.properties)
—UPDATE PROPERTIES  under sink-quickstart-Postgres.properties.
TPC-H Dataset: Obtain the TPC-H dataset or use tools to generate this data.
— Modified makefile in dbgen 
CC  = gcc (compiler., prefers using c compiler as makefile is written in c)
DATABASE = POSTGRES ( name of the database that is used)
MACHINE  = LINUX (operating system )
WORKLOAD = TPCH
— Converted data records to .csv file.
>    csv_file="${tbl_file%.tbl}.csv"
>    sed 's/|$//g' "$tbl_file" | awk -F '|' 'BEGIN {OFS=","}
      {print $0}' > "$csv_file".
Design the Streaming Data Model
Streaming Data Ingestion: Implement a method to continuously ingest TPC-H data into your streaming platform.
— Implemented a producer.py file to read the data from  TPC-H tool kit  and push it to kafka topic
Integrate PostgreSQL with the Streaming Platform
Data Push Mechanism: Develop a mechanism to push data from the streaming platform to PostgreSQL.
— Implemented a consumer.py file to push the data continuously from kafka topic to Postgres. 
Real-time Query Execution: Ensure PostgreSQL can execute queries in real-time as data streams in.
— Transform the TPC-H query to Postgres queries
Performance Evaluation
Benchmarking: Run the modified TPC-H queries against your streaming setup.
