#!/usr/bin/python3

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


c = AvroConsumer(
    {
        "bootstrap.servers": ','.join([
        "rc1b-fjeb5gtg0j5km83n.mdb.yandexcloud.net:9091",
        "rc1b-hhv399eed00oft3n.mdb.yandexcloud.net:9091",
        "rc1b-v3uq5ip03116hjhd.mdb.yandexcloud.net:9091",
        ]),
        "group.id": "avro-consumer",
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "/usr/share/ca-certificates/YandexInternalRootCA.crt",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "user-admin",
        "sasl.password": "useradmin",
        "schema.registry.url": "https://rc1b-fjeb5gtg0j5km83n.mdb.yandexcloud.net:443",
        "schema.registry.basic.auth.credentials.source": "SASL_INHERIT",
        "schema.registry.ssl.ca.location": "/usr/share/ca-certificates/YandexInternalRootCA.crt",
        "auto.offset.reset": "earliest"
    }
)

c.subscribe(["topic1"])

while True:
    try:
        msg = c.poll(10)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

c.close()
