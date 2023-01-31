# pinterest_pipeline
Data engineering pipeline from pinterest API


2. Kafka setup

- by looking through the server.properties file within kafka/config we can see the correct port to be 9092

- In kafkas current build it is impossible to run without the zookeeper, the zookeeper is responsible for brokers (servers) & partitions/leader elections

- so before we start coding up we setup our kafka zookeeper within the binary folder of kafka, here we use bash to start the zookeeper server connected to our zookeeper.properties file within the config folder, the same is done with the binary server-start linked to the config server.properties

- for this kafka-python is utilised, in order to create our pipeline we need a producer (sends data stream/ writes data) and a consumer (used to access data/ read data)

- the producer is setup, here we connect to our localhost, in the producer the bootstrap server is 9092 as specified within  server.properties, alongside this we need a value serializer, this converts the incoming data (here a dictionary) into bytes, kafka loves bytes

- within our api we utilise a post method to well... post or send data, here we are sending sata to the post method, this data is in the form of a dictionary, from here we use the produced.send method to transform this dictionary into bytes & specify the topic we want the message to be sent to