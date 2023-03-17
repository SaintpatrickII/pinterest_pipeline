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
findspark.init('/Users/paddy/spark/spark-3.3.1-bin-hadoop3')
print(findspark.find())
import multiprocessing
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import json
import sys
# spark_path = '/Users/paddy/spark/spark-3.3.1-bin-hadoop3' # spark installed folder
# os.environ['SPARK_HOME'] = spark_path
# sys.path.insert(0, spark_path + "/bin")
# sys.path.insert(0, spark_path + "/python/lib/")
# sys.path.insert(0, spark_path + "/python/lib/pyspark.zip")
# sys.path.insert(0, spark_path + "/python/lib/py4j-0.10.9.5-src.zip")
# %%
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.1,org.apache.spark:spark-sql_2.13:3.3.1,org.postgresql:postgresql:42.3.3 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.1,org.postgresql:postgresql:42.2.10 pyspark-shell'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.2.10 pyspark-shell'

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
# com.zaxxer:HikariCP:3.3.1
# postgres_ps = os.environ['POSTGRES_PASSWORD']
postgres_ps = os.environ.get('POSTGRESS_PASSWORD')

           
        

bootstrap_stream = 'localhost:9092'
stream_topic_name = 'streamTopic'

spark = SparkSession \
    .builder \
    .appName('kafkaStreaming') \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()



schema = ArrayType(StructType([
                    StructField("category", StringType(), True),
                    StructField("index", StringType(), True),
                    StructField("unique_id", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("follower_count", StringType(), True),
                    StructField("tag_list", StringType(), True),
                    StructField("is_image_or_video", StringType(), True),
                    StructField("image_src", StringType(), True),
                    StructField("downloaded", StringType(), True),
                    StructField("save_location", StringType(), True)]))



stream_pls = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', bootstrap_stream) \
    .option('subscribe', stream_topic_name) \
    .option('startingOffsets', 'earliest') \
    .load() \
    .selectExpr("CAST(value as STRING)")
stream_df =stream_pls.withColumn("temp", explode(from_json("value", schema))).select("temp.*") 
stream_df.printSchema()

stream_df = stream_df.withColumn("follower_count",regexp_replace(col("follower_count"), "k", "000"))
stream_df = stream_df.withColumn("follower_count",regexp_replace(col("follower_count"), "M", "000000"))
stream_df =stream_df.select(stream_df["category"],stream_df["follower_count"],stream_df["unique_id"])


# ds = stream_df.writeStream.outputMode('update').format('console').start().awaitTermination()   

# def foreach_batch_function(df, epoch_id):
#     url = 'jdbc:postgresql://localhost:5432/pinterest_streaming'
#     properties = {"user": "postgres", "password": postgres_ps, "driver": "org.postgresql.Driver"}
#     df.write.jdbc(url=url, table="experimental_data", mode="append", properties=properties)

# ds = stream_df.writeStream.outputMode('update').format('console').start().awaitTermination()   

mysqlUrl = "jdbc://postgresql://localhost:5432"
properties = {'user':'postgres',
              'password':str(postgres_ps),
              'driver':'com.mysql.cj.jdbc.Driver'
              }
table = 'public.experimental_data'

try:
    schemaDF = spark.read.jdbc(mysqlUrl, table, properties=properties)
    print('schema DF loaded')
except Exception:
    print('schema DF does not exist!')






#%%
# stream to console
# stream_df.writeStream.format("jdbc").foreachBatch(foreach_batch_function).outputMode('update').format('console').start().awaitTermination() 



def _write_streaming(
    df,
    epoch_id
) -> None:         

    df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'public.experimental_data') \
        .option("user", 'postgres') \
        .option("password", str(postgres_ps)) \
        .option("createTableColumnTypes", "category CHAR(64), follower_count CHAR(64), unique_id CHAR(64)") \
        .save() 

stream_df.writeStream \
    .foreachBatch(_write_streaming) \
    .start() \
    .awaitTermination()
     # .outputMode('update') \
    # .format('console') \
#%%
