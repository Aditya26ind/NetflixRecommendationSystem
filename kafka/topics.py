# """Topic creation helper."""
# from kafka.admin import KafkaAdminClient, NewTopic
# from kafka.errors import TopicAlreadyExistsError
# from .config import KAFKA_BOOTSTRAP, TOPIC_EVENTS, TOPIC_DLQ, PARTITIONS

# REPLICATION = 1


# def create_topics():
#     admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
#     topics = [
#         NewTopic(name=TOPIC_EVENTS, num_partitions=PARTITIONS, replication_factor=REPLICATION),
#         NewTopic(name=TOPIC_DLQ, num_partitions=1, replication_factor=REPLICATION),
#     ]
#     try:
#         admin.create_topics(new_topics=topics, validate_only=False)
#         print("Topics created")
#     except TopicAlreadyExistsError:
#         print("Topics already exist")
#     finally:
#         admin.close()


# if __name__ == "__main__":
#     create_topics()
