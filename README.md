# pinterest_pipeline

# 1. Project brief
The aim of this project is to create a Data engineering pipeline mimicing data from pinterest. The project begins with a preset API endpoint with a looping dataset which POST's into the api. From here it's our job to ingest this data to kafka topics, this data must then be cleaned & transformed into a usable dataframes utilising PySpark in both batch & streaming. From here the cleaned dataframes will be moved to permanent storage within AWS S3, the batch processing will be automated with Airflow for daily updates via cronjobs & the streaming data must be appended to a postgres table suitible for SQL based analysis

Key Features:

- Pipeline focusses on data clensing utilising kafka for data ingestion

- PySpark for both batch processing & stream processing

- S3 for data storage

- Airflow for batch automation

- Postgres to contain cleaned results in SQL friendly format ready for analysis


# 2. Kafka setup

What is kafka: https://medium.com/event-driven-utopia/understanding-kafka-topic-partitions-ae40f80552e8

Kafka Download: https://kafka.apache.org/downloads

- For this I had installed the scala 2.12 version to avoid any compatibility issues .From here the file is stored as a compressed archive file in downloads,  in the console a quick change to 'cd Downloads' and 'tar -xf filename -C ~' to uncompress the file & move it to the home directory

- by looking through the server.properties file within kafka/config/server_properties we can see the correct port to be 9092 as shown below, this will be the 'bootstrap_server' we connect to with Kafka, note here when referencing in code this is actually 'localhost:9092'

<img width="911" alt="Screenshot 2023-03-09 at 14 59 39" src="https://user-images.githubusercontent.com/92804317/224064081-f3bec1c8-8968-4d00-a07c-974d603b06cc.png">

- In kafkas current build it is impossible to run without the zookeeper, the zookeeper is responsible for brokers (servers) & partitions/leader elections

- so before we start coding up we setup our kafka zookeeper within the binary folder of kafka, here we use bash to start the zookeeper server connected to our zookeeper.properties file within the config folder, the same is done with the binary server-start linked to the config server.properties again as shown below
- 
<img width="1013" alt="Screenshot 2023-03-09 at 15 04 49" src="https://user-images.githubusercontent.com/92804317/224065748-57dd7660-297b-4357-a0cf-9385684685fb.png">

<img width="1013" alt="Screenshot 2023-03-09 at 15 05 54" src="https://user-images.githubusercontent.com/92804317/224065805-ff91102b-3f8a-487c-bc3a-fb67adf5e4c1.png">

- We now have an active Kafka server, but it has no idea what data to collect... lets fix that

- Thankfully kafka has a python based module this kafka-python is utilised, in order to create our pipeline we need a producer (sends data stream/ writes data) and a consumer (used to access data/ read data), as for now installs are as easy as 'pip3 install kafka-python', later we will see this gets ALOT harder

- Although we have a server we need to create a topic, this topic is just simply the name for a data injestion category i.e. pinterestDataStreaming or pinterestBatch, to create these we can create a empty topic list, append to it some new topics & them push that to the kafka admin client to create


- the producer is setup, here we connect to our localhost, in the producer the bootstrap server is 9092 as specified within  server.properties, alongside this we need a value serializer, this converts the incoming data (here a dictionary) into bytes, kafka loves bytes

- within our api we utilise a post method to well... post or send data, here we are sending data to the post method, this data is in the form of a dictionary, from here we use the produced.send method to transform this dictionary into bytes & specify the topic we want the message to be sent to

- To utilise both batch & stream processing we create two different consumer files, while almost identical, with the batch consumer we can append incoming messages to a list, setup in a loop once a messaage count is reached this loop then sends the batch off

3. Batch processing into data lake

- Now we have the batch consumer mentioned above we need some file storage a S3 bucket is made for persistant storage, this will be communicated with spark later for data cleaning

- Once the batch messsage criteria is met the batch consumer transfers the list into a json file utilising json.loads(), loads must be used to send the data to s3 in bytes format

- Using boto3 & verifying credentials with AWS CLI we can send batched data into S3

4. Processing batch data with Spark

- This took ages to successfully integrate spark & Java, on macOS many issues come up

- Homebrew is used as the main installer for both spark & java, both have to have an environmental variable setup in .zshrc file setting PATH to each, spark has to verify these under $SPARK_HOME and $JAVA_HOME 

- This becomes very difficult with homebrew not initially providing direct PATHS that work with the env variables, https://maelfabien.github.io/bigdata/SparkInstall/ Is the only guide i found to successfully get this working after (a lot) of hours

- Once spark is setup we have to setup spark to read in our saved json, firstly a os.environ arg is setup to link spark to AWS S3 via maven coodinates of both at the same versions we have

- After this we can now setup a configuration, here we name our app (i like to think of this like an instance of spark) & add it to a sparkcontext method which in turn is added to a sparksession method to start our instance

- Alongside this hadoop is utilised to perform the distributed computing we want (and is necessary to utilise pyspark), whereas pyspark is the RDD which computes transformations, here we add args to allow hadoop access to our s3 bucket via access and secret access keys set in an .env file, to note here all s3 methods utilise the s3a applications

- Now we can simply load in our json using a direct filepath from the S3 bucket

- Now all this is setup we begin cleaning, before cleaning the json appears:

<img width="1309" alt="Screenshot 2023-03-03 at 13 15 43" src="https://user-images.githubusercontent.com/92804317/222729486-359a9cc8-a7f3-4150-8cf7-ee7c2f81f3e5.png">

- For this we have a few things to sort out:

1. In our spark schema all records within string representaion are stored as objects, these will need to be cast into StringType and IntType

2. our strings contain unicode characters (highlighted in light blue), a quick encode decode utf-8 will rid the table of these

3. follower count if in thousand or million are represented with 'k' and 'M' respectfully, these will be converted into a integer value

4. Some tag list have commas seperating every character, so this will be str replaced to remove these

5. '#' are used at the beginning of tags, these will also be removed

# By the end of the data cleansing our schema will look as follows:

<img width="1176" alt="Screenshot 2023-02-28 at 11 15 04" src="https://user-images.githubusercontent.com/92804317/222729140-5364c0ee-9215-4f22-866c-b5a7843f0a83.png">


https://sandeepkattepogu.medium.com/python-spark-transformations-on-kafka-data-8a19b498b32c
-  

