#%%
from unicodedata import category
import findspark
# findspark.find()
# findspark.init()
import pyspark
import multiprocessing
import operator
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.pandas as ps
from pyspark.sql.functions import *
findspark.find()

#%%
cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
#     # Setting config value via string
#     .set("spark.eventLog.enabled", False)
#     # Setting environment variables for executors to use
#     .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
#     # Setting memory if this setting was not set previously
#     .setIfMissing("spark.executor.memory", "1g")
)

# Getting a single variable
# print(cfg.get("spark.executor.memory"))
# Listing all of them in string readable format
# # print(cfg.toDebugString())
# session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
# # rddDistributedData = session.sparkContext.parallelize([1, 2, 3, 4, 5])
# # print(rddDistributedData.collect())

# sc = session.sparkContext(conf=cfg)

# data = list(range(10,-11,-1))
# print(data)

# result = (
#     sc.parallelize(data)
#     .filter(lambda val: val % 3 == 0)
#     .map(operator.abs)
#     .fold(0, operator.add)
# )

# print(result)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell'

S3_config = (
    pyspark.SparkConf()
    .setAppName('S3app')

)



sc_s3 = pyspark.SparkContext(conf=S3_config)

accessKeyId=os.environ['AWS_ACCESS_KEY']
secretAccessKey=os.environ['AWS_SECRET_KEY']
hadoopConf = sc_s3._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')



s3_spark = SparkSession(sc_s3)


df = s3_spark.read.json("s3a://pinterest-data-a25f6b34-55e7-4a83-a1ef-4c02a809a2a9/test.json")
# df = ps.DataFrame(df)
cols_to_cast = ['category', 'unique_id', 'title', 'description', 'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 'save_location']
df = df.select([col(c).cast(StringType()) if c in cols_to_cast else c for c in df.columns])
df = df.withColumn("tag_list",regexp_replace(col("tag_list"), ",", ""))
df.show()
df.printSchema()

#%%