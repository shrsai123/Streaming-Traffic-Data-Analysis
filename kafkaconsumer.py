from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.functions import col,avg
import time

# Create a SparkSession
spark = SparkSession.builder\
        .appName("TrafficData")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")\
        .config("spark.driver.extraClassPath","/usr/share/java/mysql-connector-java-8.0.33.jar")\
        .getOrCreate()


# Read the traffic data from Kafka
trafficDF1 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","los-angeles") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")
    
trafficDF2 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","san-franscisco") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")
    
trafficDF3 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","new-york") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")


# Parse the JSON data and create a DataFrame
trafficDF1 = trafficDF1 \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'timestamp TIMESTAMP, location STRING, speed FLOAT') AS data") \
    .selectExpr("data.timestamp", "data.location", "data.speed")



trafficDF2 = trafficDF2 \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'timestamp TIMESTAMP, location STRING, speed FLOAT') AS data") \
    .selectExpr("data.timestamp", "data.location", "data.speed")

# Perform some queries using Spark SQL

trafficDF3 = trafficDF3 \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'timestamp TIMESTAMP, location STRING, speed FLOAT') AS data") \
    .selectExpr("data.timestamp", "data.location", "data.speed")
    



result1 = trafficDF1 \
    .filter(col("location").like("%Los Angeles%")) \
    .groupBy(window("timestamp", "5 minutes").alias("window"), "location") \
    .agg(avg("speed").alias("avg_speed")) \
    .filter(col("avg_speed")>10)\
    .select("window.start", "window.end", "location", "avg_speed")
    

result2 = trafficDF2 \
    .filter(col("location").like("%San Francisco%")) \
    .groupBy(window("timestamp", "5 minutes").alias("window"), "location") \
    .agg(avg("speed").alias("avg_speed")) \
    .filter(col("avg_speed")>10)\
    .select("window.start", "window.end", "location", "avg_speed")



result3 = trafficDF3 \
    .filter(col("location").like("%New York%")) \
    .groupBy(window("timestamp", "5 minutes").alias("window"), "location") \
    .agg(avg("speed").alias("avg_speed")) \
    .filter(col("avg_speed")>10)\
    .select("window.start", "window.end", "location", "avg_speed")


def write_to_mysql(df, epoch_id):
    # define the connection properties
    properties = {
        "user": "root",
        "password": ""
    }
    # write the DataFrame to MySQL
    df.write.jdbc(
        url="jdbc:mysql://localhost:3306/trafficdata",
        table="traffic_results",
        mode="append",
        properties=properties)


# define the streaming query
start_time=time.time()
result1.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .trigger(processingTime='5 seconds') \
    .start()

result2.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .trigger(processingTime='5 seconds') \
    .start()

result3.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .trigger(processingTime='5 seconds') \
    .start()

spark.streams.awaitAnyTermination(timeout=40)
end_time=time.time()
duration=end_time-start_time


print("duration",duration)

