from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata
from dataclasses import dataclass, field, asdict
from faker import Faker

import json
import random


def topic_exists(client: AdminClient, topic_name):
    """Checks if the given topic exists"""
    topic_metadata: ClusterMetadata = client.list_topics()
    result = topic_metadata.topics.get(topic_name) is not None
    return result


def _create_topic(client, topic_name, config=None, num_partitions=1, replication_factor=1):
    if config is None:
        config = {}
    futures = client.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=config
            )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise


def create_topic(client, topic_name, config: dict = None, num_partitions=1, replication_factor=1):
    """Creates the topic with the given topic name"""

    exists = topic_exists(client, topic_name)
    print(f"Topic {topic_name} exists: {exists}")

    if exists is False:
        _create_topic(client, topic_name, config, num_partitions, replication_factor)


@dataclass
class Purchase:
    faker = Faker()
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # TODO: Serializer the Purchase object
        #       See: https://docs.python.org/3/library/json.html#json.dumps
        return json.dumps(asdict(self))
