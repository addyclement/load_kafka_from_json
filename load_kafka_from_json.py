from pyspark import SparkConf, SparkContext, sql
from datetime import datetime
from pyspark.sql import SparkSession
sc = SparkSession.builder.getOrCreate()
sc.sparkContext.setLogLevel("ERROR")
#sc.set("spark.executor.memory", "24g")
sqlContext = sql.SQLContext(sc)

#create a schema for the data

dateTimeObj = datetime.now()
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print('load started : ', timestampStr)

# /usr/local/spark/jars mysql drivers here

# Read all JSON files from a folder

json_df = sc.read.option("multiline","true").json("/home/ec2-user/kafka/kafka_export/*.json")

#print the schema just for view only purpose, preview the top 3 entries

json_df.printSchema()
json_df.show(33,truncate= False)

#write the data to kafka cluster (MSK) 

json_df.selectExpr("to_json(struct(*)) AS value").write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "b-1.shoppingmsk01.xxxxx.c8.kafka.us-east-1.amazonaws.com:9092,b-2.shoppingmsk01.xxxxx.c8.kafka.us-east-1.amazonaws.com:9092") \
      .option("topic", "elk-stream") \
      .save()

#Print event date time
dateTimeObj = datetime.now()
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print('load completed : ', timestampStr)


