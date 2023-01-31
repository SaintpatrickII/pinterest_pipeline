from ensurepip import bootstrap
from sys import api_version
from kafka import KafkaConsumer, KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from json import dumps, loads
from time import sleep

# print('hi')
admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='KafkatoPython'

)

consumer = KafkaConsumer(
    'firstTopic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x : dumps(x.decode('utf-8')),
    auto_offset_reset='earliest'

)



# topic_list = []
# topic_list.append(NewTopic(name='firstTopic', num_partitions=3, replication_factor=1))
# topic_list.append(NewTopic(name='secondTopic', num_partitions=3, replication_factor=1))
# topic_list.append(NewTopic(name='thirdTopic', num_partitions=3, replication_factor=1))

# admin_client.create_topics(new_topics=topic_list)
# print(consumer.topics())

for message in consumer:
    print(message.value)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer= lambda x : bytes(x, 'utf-8')
)

# producer.send('firstTopic', 'test message')
# sleep(2)
for message in consumer:
    print(message)
    break