# Please complete the TODO items in the code
import asyncio
from asyncio import CancelledError
from concurrent.futures import Executor

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient

from helpers import create_topic

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.udacity.lesson2.exercise5.iterations"

def main():
    """Runs the exercise"""

    client = AdminClient({"bootstrap.servers": BROKER_URL})
    create_topic(client, TOPIC_NAME)
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        print(f"Deleting topic {TOPIC_NAME}")
        client.delete_topics([TOPIC_NAME]).get(TOPIC_NAME).result()
        Executor().shutdown(wait=True)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    # TODO: Set the auto offset reset to earliest
    #       See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
    )

    # TODO: Configure the on_assign callback
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.subscribe
    consumer.subscribe([topic_name], on_assign=on_assign)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")
            await asyncio.sleep(0.1)
    except CancelledError:
        print("Closing consumer")
        consumer.close()


def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    # TODO: Set the partition offset to the beginning on every boot.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.on_assign
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.TopicPartition
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    # TODO: Assign the consumer the partitions
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.assign
    consumer.assign(partitions)


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    producer = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    try:
        while True:
            producer.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
            curr_iteration += 1
            await asyncio.sleep(0.1)
    except CancelledError:
        print("Stopping producer")
        producer.flush()


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    try:
        await t1
        await t2
    except CancelledError:
        t1.cancel()
        t2.cancel()


if __name__ == "__main__":
    main()
