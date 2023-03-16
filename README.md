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

- For this I had installed the scala 2.12 version (via spark) to avoid any compatibility issues .From here the file is stored as a compressed archive file in downloads,  in the console a quick change to 'cd Downloads' and 'tar -xf filename -C ~' to uncompress the file & move it to the home directory

- by looking through the server.properties file within kafka/config/server_properties we can see the correct port to be 9092 as shown below, this will be the 'bootstrap_server' we connect to with Kafka, note here when referencing in code this is actually 'localhost:9092'

<img width="911" alt="Screenshot 2023-03-09 at 14 59 39" src="https://user-images.githubusercontent.com/92804317/224064081-f3bec1c8-8968-4d00-a07c-974d603b06cc.png">

- In kafkas current build it is impossible to run without the zookeeper, the zookeeper is responsible for brokers (servers) & partitions/leader elections

- so before we start coding up we setup our kafka zookeeper within the binary folder of kafka, here we use bash to start the zookeeper server connected to our zookeeper.properties file within the config folder, the same is done with the binary server-start linked to the config server.properties again as shown below
- 
<img width="1013" alt="Screenshot 2023-03-09 at 15 04 49" src="https://user-images.githubusercontent.com/92804317/224065748-57dd7660-297b-4357-a0cf-9385684685fb.png">

<img width="1013" alt="Screenshot 2023-03-09 at 15 05 54" src="https://user-images.githubusercontent.com/92804317/224065805-ff91102b-3f8a-487c-bc3a-fb67adf5e4c1.png">

- We now have an active Kafka server, but it has no idea what data to collect... lets fix that

- Thankfully kafka has a python based module this kafka-python is utilised, in order to create our pipeline we need a producer (sends data stream/ writes data) and a consumer (used to access data/ read data), as for now installs are as easy as 'pip3 install kafka-python', later we will see this gets ALOT harder

- Although we have a server we need to create a topic, this topic is just simply the name for a data injestion category i.e. pinterestDataStreaming or pinterestBatch, to create these we can create a empty topic list, append to it some new topics & them push that to the kafka admin client to create. here partitions are the amount of smaller storage units that hold messages within a kafka topic

<img width="568" alt="Screenshot 2023-03-09 at 15 48 35" src="https://user-images.githubusercontent.com/92804317/224077877-4619d19b-b2d7-48d2-9c98-02340a905168.png">

<img width="568" alt="Screenshot 2023-03-09 at 15 16 40" src="https://user-images.githubusercontent.com/92804317/224078064-f5941bbe-6290-4858-b23f-6dcc6f034e5b.png">

- the producer is setup, here we connect to our localhost, in the producer the bootstrap server is 9092 as specified within  server.properties, alongside this we need a value serializer, this converts the incoming data (here a dictionary) str representaion, originally i had encoded into a bytes array however when decoding this leads to alot of escaped unicode characters which are (awful) to deal with.

- the producer we created acts like a basic python class where we call the .send method taking in ('topic', datapoint) as args, so in this code for every POST request made it will be sent to both the batch & streaming topics

<img width="568" alt="Screenshot 2023-03-09 at 15 51 42" src="https://user-images.githubusercontent.com/92804317/224078743-f0db661e-ba4b-49e7-9adb-4f18f70d3e17.png">


# 3. Batch processing into data lake

- Now we have the batch consumer mentioned above we need some file storage, a S3 bucket is made for persistant storage. This will be communicated with spark later for data cleaning, here boto3 is utilised as the service required to send data to S3 via python scripts

- Access keys are generated, these are specified using AWS CLI & stored in .env file to be referenced in code (not today hackers) 

- Once the batch messsage criteria is met the batch consumer transfers the list into a json file utilising json.loads(), loads must be used to send the data to S3 as loads deserialises the str(json) we dumped before in the value serializer

<img width="652" alt="Screenshot 2023-03-09 at 16 44 46" src="https://user-images.githubusercontent.com/92804317/224094358-274fa71b-30de-4bf9-a042-01b837d7184d.png">


# 4. Processing batch data with Spark

Spark installation: https://spark.apache.org/downloads.html

Java installation: https://www.java.com/en/download/

- This took ages to successfully integrate spark & Java, on macOS many issues come up so i will try to streamline this here, homebrew can work but many issues arise from some java versions being incompatible to homebrew not providing physical filepaths so we will install both locally :)

- Java 1.8 was utilised as my java version, spark itself is installed & immediatly sent to a spark folder created within my user folder in this case '/Users/paddy/spark/spark-3.3.1-bin-hadoop3', spark when installed also installs a compatible scala version 

- Two variables must be set in order for java & spark to find each other, $SPARK_HOME and $JAVA_HOME.

- Both of these are within a hidden file within the home directory, for mac this is called .zshrc, or on most other systems .bashrc, to gain access to this we 'cd ~' to return to home directory then 'ls -a', the -a flag reveals hidden files & from here 'nano .zshrc' to ammend these new filepaths
- 
<img width="763" alt="Screenshot 2023-03-09 at 17 15 30" src="https://user-images.githubusercontent.com/92804317/224104680-a0747de9-e6fb-49ce-abcf-83dc63da4156.png">

- to find filepath & version of java installed, use commands below

<img width="563" alt="Screenshot 2023-03-09 at 17 09 49" src="https://user-images.githubusercontent.com/92804317/224104732-65d64be5-74df-41b2-a9e6-7d8643b55161.png">

once in a new python file we can import findspark & run findspark.find() which will return our spark path, to double check spark is working in the console we can run spark -shell

<img width="820" alt="Screenshot 2023-03-09 at 17 28 11" src="https://user-images.githubusercontent.com/92804317/224107514-24b28a63-40c2-4adc-b4b3-75f07c31cc24.png">

- Once spark is setup we have to setup spark to read in our saved json, firstly a os.environ arg is setup to link spark to AWS S3 via maven coodinates of both at the same versions we have

- what is a maven coordinate? Spark itself cannot access or send data to different destinations i.e. cloud services by itself, we need 'spark connectors', the maven repository contains the necessary coordinates to interact with these different services, we have to tell spark how to find these

- for our project we need to pass PYSPARK_SUBMIT_ARGS to update maven coordinates for the aws-java-sdk and hadoop-aws
-
<img width="846" alt="Screenshot 2023-03-09 at 17 42 02" src="https://user-images.githubusercontent.com/92804317/224111007-9e8f74c2-2a78-4c92-9247-4e6c0f42e782.png">

- After this we can now setup a configuration, here we name our app (i like to think of this like an instance of spark) & add it to a sparkcontext method which creates a connection to spark with our configuration which in turn is added to a sparksession method to start our instance

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

# 5. Orchestrate batch processing using airflow

- Airflow is a task scheduler & monitoring tool utilised heavily for managing automated tasks, For this instance we want to create a workflow that simply consumes new kafka messages to upload to S3 & then batch processes the updated results using pyspark

- Airflow creates workflows via DAG's (Directed Acyclic Graphs), this allows tasks to be completed based on prerequisite tasks i.e. 'only perform task B is task A has succeded'

- Once Airflow is installed & the Airflow folder created in the home directory these DAG's are stored within the 'dags' folder within the airflow directory, if we look in the airflow.cfg we can see that this is the default path airflow will look for jobs/dags once run

<img width="642" alt="Screenshot 2023-03-16 at 10 22 30" src="https://user-images.githubusercontent.com/92804317/225587870-d371f18a-da7e-4ae1-aa19-deca552eff38.png">

- DAG's can be created using python scripting (Within the dags folder!) & utilise bashOperators to move through directories & execute commands, default_args are used to sign into airflow from the local system

- a DAG class is created taking in user login information as an arg, aslo in these args jobs can be scheduled utilising cronjobs labeled 'schedule_interval' arguement, for this we want the job to schedule everyday at midday so '0 12 * * *' by using https://crontab.guru

<img width="802" alt="Screenshot 2023-03-16 at 10 33 12" src="https://user-images.githubusercontent.com/92804317/225590669-1c167199-6385-494d-bd00-7144a71a84b1.png">

- For airflow's UI to be visible we need to assign it to a port using 'airflow webserver --port 8080' to allow a localhost:8080 connection, then running 'airflow scheduler' will allow refreshing of DAG code

# Lets create a few basic commands:

<img width="802" alt="Screenshot 2023-03-16 at 10 33 12" src="https://user-images.githubusercontent.com/92804317/225591862-50a520e6-67f8-4365-8262-e812898e5fbf.png">

-Here we are simply running both our kafka batch upload to S3 file & then running the file for cleaning said S3 data with pyspark

- If we look at airlow we can see the progress of jobs, a graph view of task ordering & also the code which airflow is using for the DAG

![Screenshot 2023-03-16 at 10 31 56](https://user-images.githubusercontent.com/92804317/225592488-9ac63077-797f-4915-bcf0-7c152c3c4ba9.png)

![Screenshot 2023-03-16 at 10 31 44](https://user-images.githubusercontent.com/92804317/225592528-0ce7016f-8ba1-4590-b076-7d1521d46311.png)

![Screenshot 2023-03-16 at 10 32 34](https://user-images.githubusercontent.com/92804317/225592579-708e995d-2a79-42d9-b0eb-270809c6d054.png)


# 6. Spark Streaming

- Streaming alike batch begins with setting a SparkSession however we now have to set our spark to read the stream, to do thus we need the bootstrap, topic name ( to subscribe to the topic), & we want to load the stream  from here thanks to sparks RDD integration we can call the function like a dataframe to append with no issues

- Notice here we did some minor feature selection in only cleaning some of the data, for this i simply chose: category, follower_count, downloaded for an easy model if we were to train it

<img width="799" alt="Screenshot 2023-03-09 at 18 09 30" src="https://user-images.githubusercontent.com/92804317/224117129-f7a57071-6e3e-4bbe-860e-fff58ce6cc4c.png">


<img width="602" alt="Screenshot 2023-03-09 at 12 42 25" src="https://user-images.githubusercontent.com/92804317/224117212-87951cb8-04ab-4b94-b6e6-94a6bad29477.png">

# 7. Storage

Using postgres we now create a new database titlied 'pinterest_streaming' & a table 'experimental_data' where we will stream this data to

