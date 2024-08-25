import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg
from confluent_kafka import Consumer
from pymongo import MongoClient
import time

# Initialize MongoDB client
client = MongoClient("mongodb://localhost:27017/")
db = client["dbt_h"]  # Replace "your_database" with your actual database name

spark = SparkSession.builder \
    .appName("AverageTurbidity") \
    .getOrCreate()

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cold_consumer_group',  # Consumer group for the "cold" topic
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['cold'])  # Subscribe to the "cold" topic

# Initialize time of last print
last_print_time = time.time()

# Counter to generate collection names
collection_counter = 1

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Parse the JSON message
        data_dict = json.loads(msg.value().decode("utf-8"))

        # Extract relevant fields from the JSON data
        timestamp = data_dict['timestamp']
        temperature = data_dict['temperature']
        turbidity = data_dict['turbidity']

        # Determine the collection name based on the temperature
        collection_name = f"raw_data_{collection_counter}"

        # Get or create the collection
        collection = db[collection_name]

        # Insert raw data into MongoDB
        collection.insert_one(data_dict)

        # Create DataFrame with the parsed data
        df = spark.createDataFrame([(timestamp, temperature, turbidity)], ["TimeStamp", "Temperature", "Turbidity"])

        # Calculate average turbidity over a window of 10 minutes
        window_duration = "15 minutes"
        w = df.groupBy(window(df["TimeStamp"], window_duration)).agg(avg(df["Turbidity"]).alias("AvgTurbidity"))

        # Check if 10 minutes have passed since the last print
        current_time = time.time()
        elapsed_time = current_time - last_print_time
        if elapsed_time >= 900:  
            # Show the result
            print(w.collect())
            
            # Update last print time
            last_print_time = current_time
            collection_counter += 1

        # Increment the collection counter


except KeyboardInterrupt:
    pass

finally:
    consumer.close()
    spark.stop()

