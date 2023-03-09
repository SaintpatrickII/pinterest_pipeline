# %%
from ensurepip import bootstrap
from lib2to3.pgen2 import pgen
from sys import api_version
from grpc import protos_and_services
from kafka import KafkaConsumer, KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from json import dumps, loads
from time import sleep
import findspark
import multiprocessing
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import json
findspark.init()
findspark.find()
# %%
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 kafka_consumer_stream.py pyspark-shell'

# session = pyspark.sql.SparkSession.builder.config(
#     conf=pyspark.SparkConf()
#     .setMaster(f"local[{multiprocessing.cpu_count()}]")
#     .setAppName("SparkStreaming")
# ).getOrCreate()

# ssc = StreamingContext(session.sparkContext, batchDuration=30)

# test_port = ssc.socketTextStream('localhost', 9999)

# ssc.start()

# # stops after specified time 
# seconds = 180
# ssc.awaitTermination(seconds)

bootstrap_stream = 'localhost:9092'
stream_topic_name = 'streamTopic'

spark = SparkSession \
    .builder \
    .appName('kafkaStreaming')\
    .getOrCreate()

# spark_postgres = SparkSession.builder.config("spark.jars", "/usr/local/postgresql-42.5.4.jar") \
#     .master ("local") \
#     .appName ("PySparkPostgres") \
#     .getOrCreate()

cols_to_cast_str = ['category']
cols_to_cast_int = ['unique_id',  'follower_count', 'downloaded']



cols_to_cast = ['category', 'unique_id', 'title', 'description', 'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'downloaded']
schema = StructType([ 
        StructField("category", StringType(), True),
        StructField("follower_count" , StringType(), True),
        StructField("downloaded" , StringType(), True),
        StructField("unique_id" , StringType(), True),
        ])


stream_df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', bootstrap_stream) \
    .option('subscribe', stream_topic_name) \
    .option('startingOffsets', 'earliest') \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("parsed_value")) \
    .select(col("parsed_value.*")) \

stream_df = stream_df.withColumn("follower_count",regexp_replace(col("follower_count"), "k", "000"))
stream_df = stream_df.withColumn("follower_count",regexp_replace(col("follower_count"), "M", "000000"))

postgres_ps = os.environ['POSTGRES_PASSOWRD']

# pg = spark_postgres.write \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/pinterest_streaming") \
#     .option("dbtable", "tablename") \
#     .option("user", "postgres") \
#     .option("password", postgres_ps) \
#     .option("driver", "org.postgresql.Driver") \
#     .save()

stream_df.printSchema()


stream_df = stream_df.select('*')
# stream_df = stream_df \
#     .selectExpr("CAST(value AS STRING) as json")

ds = stream_df.writeStream.outputMode('append').format('console').start().awaitTermination()   





# stops only after all data consumed to port 
# ssc.stop(stopGraceFully=True)

# %%
# print('hi')
admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='KafkatoPython'

)

# consumer = KafkaConsumer(
#     'firstTopic',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda x : dumps(x.decode('utf-8')),
#     auto_offset_reset='earliest'

# )

consumer_stream = KafkaConsumer(
    'streamTopic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x : dumps(x.decode('utf-8')),
    auto_offset_reset='earliest',
    
    

)

# consumer_batch = KafkaConsumer(
#     'batchTopic',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda x : dumps(x.decode('utf-8')),
#     auto_offset_reset='earliest',
    
    

# )




# topic_list = []
# # # topic_list.append(NewTopic(name='firstTopic', num_partitions=3, replication_factor=1))
# # # topic_list.append(NewTopic(name='secondTopic', num_partitions=3, replication_factor=1))
# topic_list.append(NewTopic(name='streamTopic', num_partitions=3, replication_factor=1))

# admin_client.create_topics(new_topics=topic_list)
# print(consumer.topics())

# for message in consumer:
#     print(message.value)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer= lambda x : bytes(x, 'utf-8')
)

# producer.send('firstTopic', 'test message')
# sleep(2)
# for message in consumer_stream:
#     print(message.value)
    

# for message in consumer_batch:
#     print(message.value)
#     break

