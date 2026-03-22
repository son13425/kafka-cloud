#!/usr/bin/python3

from confluent_kafka import Consumer

if __name__ == "__main__":
    consumer_conf = {
        "bootstrap.servers": ",".join([
            "rc1b-fjeb5gtg0j5km83n.mdb.yandexcloud.net:9091",
            "rc1b-hhv399eed00oft3n.mdb.yandexcloud.net:9091",
            "rc1b-v3uq5ip03116hjhd.mdb.yandexcloud.net:9091",
        ]),
        "group.id": "nifi-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 6000,
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "user-admin",
        "sasl.password": "useradmin",
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(["nifi-topic"])

    print("Консьюмер запущен, ожидаю сообщения из nifi-topic...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка: {msg.error()}")
                continue
            value = msg.value().decode("utf-8")
            print(
                f"Получено сообщение: value={value}, "
                f"partition={msg.partition()}, offset={msg.offset()}"
            )
    except KeyboardInterrupt:
        print("\nОстановка консьюмера...")
    finally:
        consumer.close()
