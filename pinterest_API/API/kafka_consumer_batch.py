from ensurepip import bootstrap
from sys import api_version
from kafka import KafkaConsumer, KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from json import dumps, loads
from time import sleep
import boto3
import uuid
import logging
from botocore.exceptions import ClientError
import os
import tempfile
import json
from pydantic import Json





# Creating a connection to S3
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# Creating a KafkaAdminClient object that will be used to create a topic
admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='KafkatoPython'
)


# Creating a Kafka consumer that will read from the topic `batchtest`
consumer_batch = KafkaConsumer(
    'batchtest',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x : loads(x),
    auto_offset_reset='earliest',
    max_poll_records=10,
    fetch_max_bytes=20,
    fetch_max_wait_ms=10000
)    


class batch_to_s3:
    def __init__(self, kafka_consumer) -> None:
        self.consumer = kafka_consumer

    
    def consumer_to_s3(self):
        """
        This function reads messages from a Kafka topic cleans characters
        and writes them to a file in S3 once reaches 50 messages
        """
        data = []
        with open("test.json", 'w') as f:
            for message in self.consumer:
                data.append((message.value))
                if len(data) >= 50:
                    print(len(data))
                    print(type(f.name))
                    s3_client.put_object(Body=json.dumps(data, separators=(',', ':'), ensure_ascii=True),
                                        Bucket='pinterest-data-a25f6b34-55e7-4a83-a1ef-4c02a809a2a9',
                                        Key=str(f.name)
                                        )
                    print(f'{len(data)} messages have been sent to S3')
                    data.clear()
                    break


if __name__ == '__main__':
    batch_consumer = batch_to_s3(consumer_batch)
    batch_consumer.consumer_to_s3()
