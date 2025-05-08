from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.functions import from_unixtime,window,col,avg,to_timestamp,expr
import time

 
spark = SparkSession.builder.appName("TrafficData").config("spark.driver.extraClassPath","/usr/share/java/mysql-connector-java-8.0.30.jar").getOrCreate()

        
trafficDF = spark.read \
           .format("jdbc")\
           .option("url", "jdbc:mysql://localhost/trafficdata") \
           .option("dbtable","traffic_data") \
           .option("user", "root") \
           .option("password","your_new_password")\
           .load() 




# Aggregate the data using a tumbling window of 5 minutes 

result1 = trafficDF \
    .filter(col("location").like("%Los Angeles%")) \
    .groupBy(window("timestamp", "5 minutes").alias("window"), "location") \
    .agg(avg("speed").alias("avg_speed")) \
    .filter(col("avg_speed")>10)\
    .select("window.start", "window.end", "location", "avg_speed")
    
result2 = trafficDF \
    .filter(col("location").like("%San Francisco%")) \
    .groupBy(window("timestamp", "5 minutes").alias("window"), "location") \
    .agg(avg("speed").alias("avg_speed")) \
    .filter(col("avg_speed")>10)\
    .select("window.start", "window.end", "location", "avg_speed")

result3 = trafficDF \
    .filter(col("location").like("%New York%")) \
    .groupBy(window("timestamp", "5 minutes").alias("window"), "location") \
    .agg(avg("speed").alias("avg_speed")) \
    .filter(col("avg_speed")>10)\
    .select("window.start", "window.end", "location", "avg_speed")

result1.createOrReplaceTempView("result1_temp_view")
result2.createOrReplaceTempView("result2_temp_view")
result3.createOrReplaceTempView("result3_temp_view")


start_time=time.time()


spark.sql("SELECT * FROM result1_temp_view").show()
spark.sql("SELECT * FROM result2_temp_view").show()
spark.sql("SELECT * FROM result3_temp_view").show()

end_time=time.time()
duration=end_time-start_time
print("duration",duration)
