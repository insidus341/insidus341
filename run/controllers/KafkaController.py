from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps


def KafkaConsumerLocal(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='assignment',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    return consumer


def KafkaProducerLocal():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x:
        dumps(x).encode('utf-8')
    )

    return producer
