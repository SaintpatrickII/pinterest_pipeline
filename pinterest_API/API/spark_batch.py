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
from pyspark.sql.types import StringType, IntegerType
import pyspark.pandas as ps
from pyspark.sql.functions import *
findspark.find()
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell'


# Setting the SparkConf for the SparkContext.
S3_config = (
    pyspark.SparkConf()
    .setAppName('S3app')

)

# Creating a SparkContext object.
sc_s3 = pyspark.SparkContext(conf=S3_config)


# Setting up the credentials for the AWS S3 bucket and setting up spark connectors to S3.
AWS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET_KEY")
hadoopConf = sc_s3._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', AWS_KEY)
hadoopConf.set('fs.s3a.secret.key', AWS_SECRET)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')



# Creating a spark session object.
s3_spark = SparkSession(sc_s3)



def ascii_ignore(x):
    """
    It takes a string and returns a string with all non-ascii characters removed
    
    :param x: The string to be encoded
    :return: the string x, encoded as ascii, ignoring any non-ascii characters.
    """
    return x.encode('ascii', 'ignore').decode('ascii')

# Creating a user defined function that will be used to remove non-ascii characters from the
# dataframe. spark cannot directly understand python functions so they need to be wrapped in udf
ascii_udf = udf(ascii_ignore)

df = s3_spark.read.json("s3a://pinterest-data-a25f6b34-55e7-4a83-a1ef-4c02a809a2a9/test.json")
cols_to_cast = ['category', 'unique_id', 'title', 'description', 'tag_list', 'is_image_or_video', 'image_src', 'save_location']
df = df.select([col(c).cast(StringType()) if c in cols_to_cast else c for c in df.columns])
df = df.withColumn("tag_list",regexp_replace(col("tag_list"), ",", ""))
df = df.withColumn("description",regexp_replace(col("description"), "#", ""))
df = df.withColumn("tag_list", ascii_udf('tag_list'))
df = df.withColumn("tag_list", ascii_udf('tag_list'))
df = df.withColumn("title", ascii_udf('title'))
df = df.withColumn("description", ascii_udf('description'))
df = df.withColumn("follower_count",regexp_replace(col("follower_count"), "k", "000"))
df = df.withColumn("follower_count",regexp_replace(col("follower_count"), "M", "000000"))
df = df.withColumn("follower_count",regexp_replace(col("follower_count"), "User Info Error", "0"))
int_cols_to_cast = [ 'follower_count', 'downloaded', 'index']
df = df.select([col(c).cast(IntegerType()) if c in int_cols_to_cast else c for c in df.columns])


df.show()
df.printSchema()

#%%