import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from helpers import create_topic, Purchase

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.udacity.lesson2.exercise6.purchases"


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    create_topic(client, TOPIC_NAME)

    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        #
        # TODO: Write a loop that uses consume to grab 5 messages at a time and has a timeout.
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.consume
        # TODO: Print something to indicate how many messages you've consumed. Print the key and value of
        #       any message(s) you consumed
        messages = c.consume(5, 1.0)
        print(f"Got {len(messages)} messages from Kafka")
        for message in messages:
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")

        # Do not delete this!
        await asyncio.sleep(0.01)


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        for _ in range(10):
            p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(0.3)


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


if __name__ == "__main__":
    main()
