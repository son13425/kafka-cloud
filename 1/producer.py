#!/usr/bin/python3

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


value_schema_str = """
{
    "namespace": "my.test",
    "name": "value",
    "type": "record",
    "fields": [
        {
            "name": "name",
            "type": "string"
        }
    ]
}
"""

key_schema_str = """
{
    "namespace": "my.test",
    "name": "key",
    "type": "record",
    "fields": [
        {
            "name": "name",
            "type": "string"
        }
    ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


avroProducer = AvroProducer(
    {
        "bootstrap.servers": ','.join([
            "rc1b-fjeb5gtg0j5km83n.mdb.yandexcloud.net:9091",
            "rc1b-hhv399eed00oft3n.mdb.yandexcloud.net:9091",
            "rc1b-v3uq5ip03116hjhd.mdb.yandexcloud.net:9091",
        ]),
        "security.protocol": 'SASL_SSL',
        "ssl.ca.location": '/usr/share/ca-certificates/YandexInternalRootCA.crt',
        "sasl.mechanism": 'SCRAM-SHA-512',
        "sasl.username": 'user-admin',
        "sasl.password": 'useradmin',
        "on_delivery": delivery_report,
        "schema.registry.basic.auth.credentials.source": 'SASL_INHERIT',
        "schema.registry.url": 'https://rc1b-fjeb5gtg0j5km83n.mdb.yandexcloud.net:443',
        "schema.registry.ssl.ca.location": "/usr/share/ca-certificates/YandexInternalRootCA.crt"
    },
    default_key_schema=key_schema,
    default_value_schema=value_schema
)

for i in range(50):
    key = {"name": "Key-{}".format(i)}
    value = {"name": "Value-{}".format(i)}
    avroProducer.produce(topic="topic1", key=key, value=value)
    avroProducer.poll(0)  # обработка колбэков без блокировки
    print("Sent message {}".format(i))


avroProducer.flush()
print("All 50 messages sent successfully!")

