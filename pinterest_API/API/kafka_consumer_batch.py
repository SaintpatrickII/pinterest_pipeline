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



s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
# pinterest_bucket = s3_client.bucket('pinterest-data-a25f6b34-55e7-4a83-a1ef-4c02a809a2a9')

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

consumer_batch = KafkaConsumer(
    'batchTopic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x : dumps(x.decode('utf-8')),
    auto_offset_reset='earliest',
    max_poll_records=10,
    fetch_max_bytes=20,
    fetch_max_wait_ms=10000
    

)



# topic_list = []
# topic_list.append(NewTopic(name='firstTopic', num_partitions=3, replication_factor=1))
# topic_list.append(NewTopic(name='secondTopic', num_partitions=3, replication_factor=1))
# topic_list.append(NewTopic(name='thirdTopic', num_partitions=3, replication_factor=1))

# admin_client.create_topics(new_topics=topic_list)
# # print(consumer.topics())

# for message in consumer:
#     print(message.value)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer= lambda x : bytes(x, 'utf-8')
)

# producer.send('firstTopic', 'test message')
# sleep(2)

    # print(message.value)
    # mes = lambda message : json.dumps(message).encode('utf-8')
    
data = []
# with open(f"{uuid.uuid4()}.json", 'w') as f:
with open("test.json", 'w') as f:
    for message in consumer_batch:
        
        data.append(str(message.value))
        # print(data)
        if len(data) >= 100:
            #  print(data)
            # for item in data:
                # f.write(item)
                # f.write('\n')
            json_not_binary = json.dump(data, f)
            json_bin = json.dumps(json_not_binary, separators=(',', ':'), ensure_ascii=True)
            print(len(data))
            # f.write(data)
            #     f.write('\n')
            # print(f)
            print(type(f.name))
            
            # s3_client.upload_file(str(f.name), 'pinterest-data-a25f6b34-55e7-4a83-a1ef-4c02a809a2a9', str(f.name))
            s3_client.put_object(Body=json.dumps(data),
                                Bucket='pinterest-data-a25f6b34-55e7-4a83-a1ef-4c02a809a2a9',
                                Key=str(f.name)
                                )
            data.clear()
            break
        # while len(data) >= 5:
        #     print(type(message))
        #     mes = str(message.value)
        #     print(type(mes))
        #     
        #     break





    # print(type(mes))
    # mes = bytes(mes, 'utf-8')
    # print(type(mes))
    # with tempfile.TemporaryFile() as tmpfile:
    #     f = tmpfile.write(mes)
    #     # fs = f.read(f)
    #     print(f)
    #     s3_client.upload_file(str() + '.json', 'pinterest-data-a25f6b34-55e7-4a83-a1ef-4c02a809a2a9', message)
    #     break
    