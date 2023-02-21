# Please complete the TODO items in the code.
from datetime import datetime
import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from helpers import create_topic, Purchase

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise2.4.purchases"

async def produce_async(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL,
                  "linger.ms": 60000,
                  "compression.type": "lz4",
                  "batch.num.messages": 10000,
                  "queue.buffering.max.messages": 100000,
                  # "queue.buffering.max.kbytes: "",
                  })

    curr_iteration = 0
    start_time = datetime.utcnow()

    # TODO: Write a synchronous production loop.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    while True:
        # TODO: Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        #       sending it to Kafka!
        p.produce(topic_name, Purchase().serialize())

        if curr_iteration % 1000000 == 0:
            elapsed = (datetime.utcnow() - start_time).seconds
            print(f"Messages sent: {curr_iteration}, Total elapsed seconds: {elapsed}")
        curr_iteration += 1

        p.poll(0)

        # Do not delete this!
        #await asyncio.sleep(0.01)


def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    create_topic(client, TOPIC_NAME, num_partitions=5, replication_factor=1)
    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce_async(TOPIC_NAME))
    t2 = asyncio.create_task(_consume(TOPIC_NAME))
    await t1
    await t2


async def _consume(topic_name):
    """Consumes produced messages"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    num_consumed=0
    while True:
        msg = c.consume(timeout=0.001)
        if msg:
            num_consumed += 1
            if num_consumed % 100 == 0:
                print(f"consumed {num_consumed} messages")
        else:
            await asyncio.sleep(0.01)


if __name__ == "__main__":
    main()
