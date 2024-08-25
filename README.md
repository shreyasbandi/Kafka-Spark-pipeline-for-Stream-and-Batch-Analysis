
# Project Setup

This guide provides instructions for setting up the necessary Python libraries for this project, including Flask, PyMongo, Confluent Kafka, and PySpark.

## Prerequisites

Ensure you have `pip3` installed. If not, you can install it by following the instructions [here](https://pip.pypa.io/en/stable/installation/).

## Python Version

Any version of Python as of April 17, 2024, should be compatible with these installations.

## Installation Instructions

Open your terminal and run the following commands to install the required libraries:

### Flask
[Flask](https://flask.palletsprojects.com/en/2.0.x/) is a micro web framework written in Python.

```sh
pip3 install flask
```

### PyMongo
[PyMongo](https://pymongo.readthedocs.io/en/stable/) is a Python distribution containing tools for working with MongoDB.

```sh
pip3 install pymongo
```

### Confluent Kafka
[Confluent Kafka](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html) is a Python client for Apache Kafka.

```sh
pip3 install confluent_kafka
```

### PySpark
[PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/index.html) is the Python API for Apache Spark.

```sh
pip3 install pyspark
```

## Resources

- [Apache Spark Tutorial](https://www.youtube.com/watch?v=ei_d4v9c2iA&t=308s)
- [MongoDB Tutorial](https://www.youtube.com/watch?v=ZiQPyD82ojk&t=150s)
- [Apache Kafka Tutorial](https://www.youtube.com/watch?v=Pty5BWJUwMU&t=409s)

Follow the above tutorials for a comprehensive guide on using Spark, MongoDB, and Kafka in your projects.

## Verification

After installation, you can verify the installations by importing the libraries in a Python script or an interactive Python session:

```python
import flask
import pymongo
import confluent_kafka
import pyspark
```

If there are no import errors, the libraries have been installed successfully.

## Conclusion

You have now installed the necessary libraries for Flask, PyMongo, Confluent Kafka, and PySpark. Refer to the provided resources for detailed tutorials on how to use these libraries in your projects.

## Running The Project
1.Create 3 topics called hot,cold and medium these 3 shall be the topics to which kafka writes depending on the temperature.
```.sh
./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic cold
./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic hot
./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic medium
```

2.Run the sensor.py file which will simulate the working of a iot-sensor which will measure turbidity,temperature,battery-life,measurement-id and timestamp.

3.Run the push_data_to_kafka.py which will write to the 3 topics from stream-data depending on the water-temperature.

4.Now run each of the hot.py,cold.py and medium.py which will do streaming-analysis using streaming-spark and store data in mongoDB for future batch processing.

5.Run the individual files for batch processing which will do batch processing.We can then compare the time taken as well as accuracy of both and associated tradeoff by making changes in parameters in code.
