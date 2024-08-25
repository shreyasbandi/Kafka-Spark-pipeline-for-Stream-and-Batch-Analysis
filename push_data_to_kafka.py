import time
import json
import requests
import datetime
from confluent_kafka import Producer

def get_sensor_data_stream():
    try:
        url = 'http://0.0.0.0:3030/sensordata'
        r = requests.get(url)
        return r.text
    except:
        return "Error in Connection"

def get_topic_for_temperature(temperature):
    if temperature >= 0 and temperature < 10:
        return "cold"
    elif temperature >= 10 and temperature < 20:
        return "medium"
    else:
        return "hot"

p = Producer({'bootstrap.servers': 'localhost:9092'})

while True:
    msg = get_sensor_data_stream()
    # Split the sensor data message into its components
    timestamp, temperature, turbidity, battery_life, beach, measurement_id = msg.split()

    # Convert temperature to float for comparison
    temperature = float(temperature)

    # Determine the topic based on temperature range
    topic = get_topic_for_temperature(temperature)

    # Create a JSON object for the sensor data
    sensor_data = {
        "timestamp": timestamp,
        "temperature": float(temperature),
        "turbidity": float(turbidity),
        "battery_life": float(battery_life),
        "beach": beach,
        "measurement_id": int(measurement_id)
    }

    # Produce the JSON object to the determined topic
    p.produce(topic, json.dumps(sensor_data).encode('utf-8'))

    # Sleep for 1 second before fetching next sensor data
    time.sleep(1)

