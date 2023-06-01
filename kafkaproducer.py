from kafka import KafkaProducer
import datetime
from pyspark.sql import SparkSession
import json
# Set up the Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,11,5))
spark = SparkSession.builder.appName("TrafficData").config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.0.33.jar").getOrCreate()  
db_url = "jdbc:mysql://localhost/trafficdata"
db_table = "traffic_data"
db_properties = {"user": "root", "password": ""}
df = spark.read.jdbc(db_url, db_table, properties=db_properties)

# Convert data to JSON and publish to Kafka
for record in df.toJSON().collect():
    producer.send("new-york", record.encode())
    producer.send("san-franscisco",record.encode())
    producer.send("los-angeles",record.encode())
print("Topics Successfully sent")





