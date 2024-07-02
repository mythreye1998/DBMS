from confluent_kafka import Consumer, KafkaError
import psycopg2

# Initialize Kafka consumer
c = Consumer({
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'tpch_group',
    'auto.offset.reset': 'earliest'  # Start reading from the earliest messages
})
c.subscribe(['tpch'])  # Subscribe to your Kafka topic 'tpch'

# Initialize PostgreSQL connection
conn = psycopg2.connect(database="TPCHData", user="postgres", password="12345", host="localhost", port="5432")
cur = conn.cursor()

# Consume messages and insert into PostgreSQL tables
while True:
    msg = c.poll(1.0)
    #print("If it is true")
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    #print("ntg1")
    # Parse the message by splitting on the appropriate delimiter
    message_value = msg.value().decode('utf-8')
    fields = message_value.split('|')  # Adjust the delimiter as need
    
    # Debug: Check if the number of fields matches the table schema
    if len(fields) == 16:
        print("Inserting into 'lineitem' table:", fields)
            # Insert data into 'lineitem' table
        cur.execute("""
            INSERT INTO lineitem (
                L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, 
                L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, 
                L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, 
                L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(fields))
    if len(fields) == 7:
        # Insert data into 'supplier' table with 8 fields
        print("Inserting into 'supplier' table:", fields)
        cur.execute("""
            INSERT INTO supplier (
                S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(fields))
    if len(fields) == 3:
        # Insert data into 'region' table with 3 fields
        print("Inserting into 'region' table:", fields)
        cur.execute("""
            INSERT INTO region (
                R_REGIONKEY, R_NAME, R_COMMENT
            ) VALUES (%s, %s, %s)
        """, tuple(fields))  

    elif len(fields) == 5:
        print("Inserting into 'partsupp' table:", fields)
        cur.execute("""
            INSERT INTO partsupp (
                PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT
            ) VALUES (%s, %s, %s, %s, %s)
        """, tuple(fields))

        
    elif len(fields) == 4:
        # Insert data into 'nation' table with 4 fields
        print("Inserting into 'nation' table:", fields)
        cur.execute("""
            INSERT INTO nation (
                N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT
            ) VALUES (%s, %s, %s, %s)
        """, tuple(fields))
    elif len(fields) == 8:
        # Insert data into 'customer' table with 8 fields
        print("Inserting into 'customer' table:", fields)
        cur.execute("""
            INSERT INTO customer (
                C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(fields))
    elif len(fields) == 9:
    # Assuming the O_ORDERPRIORITY values have a distinct pattern like "1-URGENT"
        if any(keyword in fields[5] for keyword in ["URGENT", "HIGH", "MEDIUM", "LOW", "NOT SPECIFIED"]):
            try:
                fields[7] = int(fields[7])  # Convert O_SHIPPRIORITY to integer
                print("Inserting into 'orders' table:", fields)
                cur.execute("""
                    INSERT INTO orders (
                        O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE,
                        O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(fields))
            except ValueError:
                print("Error processing 'orders' table record:", fields)
        else:
            print("Inserting into 'part' table:", fields)
            cur.execute("""
                INSERT INTO part (
                    P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, 
                    P_CONTAINER, P_RETAILPRICE, P_COMMENT
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(fields))    

    conn.commit()

# Close database connection and Kafka consumer
cur.close()
conn.close()
