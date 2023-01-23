from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

kafka_admin = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='firstkafkaproducer'

)

producer = kafka_admin()