# pinterest_pipeline
Data engineering pipeline from pinterest API


2. Kafka setup

- by looking through the server.properties file within kafka/config we can see the correct port to be 9092

- In kafkas current build it is impossible to run without the zookeeper, the zookeeper is responsible for brokers (servers) & partitions/leader elections

- so before we start coding up we setup our kafka zookeeper within the binary folder of kafka, here we use bash to start the zookeeper server connected to our zookeeper.properties file within the config folder, the same is done with the binary server-start linked to the config server.properties

- for this kafka-python is utilised, in order to create our pipeline we need a producer (sends data stream/ writes data) and a consumer (used to access data/ read data)

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

