import json
import logging

from kafka import KafkaConsumer, KafkaProducer
# from pymongo import database, MongoClient
import psycopg2
from app.Utils import log_performance

logger = logging.getLogger(__name__)


class Resources:
    TOPIC_RAW_DATA = 'raw_topic'
    TOPIC_COMPUTED_DATA = 'computed_data'
    KAFKA_URI = 'kafka:9092'
    KAFKA_LOCAL_URI = 'localhost:9093'
    MONGODB_URI = 'mongodb://root:example@mongo:27017'

    def __init__(self) -> None:
        # Init privates
        self.__kafka_consumer = False
        self.__kafka_producer = False
        self.__storage = False

    def get_kafka_consumer(self):
        """
        Get kafka consumer, only first use make conn.
        :return:
        """
        self.__kafka_consumer = self.__kafka_consumer if self.__kafka_consumer else self.__connect_kafka_consumer()
        return self.__kafka_consumer

    def get_kafka_producer(self):
        """
        Get kafka producer, only first use make conn.
        :return:
        """
        self.__kafka_producer = self.__kafka_producer if self.__kafka_producer else self.__connect_kafka_producer()
        return self.__kafka_producer

    def get_storage(self):
        """
        Get storage, only first use make conn.
        :return:
        """
        self.__storage = self.__storage if self.__storage else self.__connect_storage()
        return self.__storage

    @log_performance
    def __connect_kafka_consumer(self) -> KafkaConsumer:
        """
        Connect to kafka service as consumer.
        :return: KafkaConsumer
        """
        return KafkaConsumer(
            self.TOPIC_COMPUTED_DATA,
            bootstrap_servers=self.KAFKA_URI,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    @log_performance
    def __connect_kafka_producer(self) -> KafkaProducer:
        """
        Connect to kafka service as producer.
        :return: KafkaProducer
        """
        return KafkaProducer(
            bootstrap_servers=self.KAFKA_URI,
            compression_type='gzip',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    @log_performance
    def __connect_storage(self):
        # """
        # Connect to MongoDB
        # :return: Database Object
        # """
        # logger.debug('Connecting to MongoDB..')
        # client = MongoClient(self.MONGODB_URI)
        # return client.ESP
        logger.debug('Connecting to Postgres..')
        connection = psycopg2.connect(user="postgres",
                                      password="example",
                                      host="postgres",
                                      port="5432",
                                      database="esp")
        cursor = connection.cursor()
        # Execute a command: this creates a new table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS public.computed_data (
            id BIGSERIAL PRIMARY KEY     NOT NULL,
            event_time timestamp with time zone,
            action TEXT NOT NULL,
            avg_duration_ms NUMERIC NOT NULL,
            count BIGINT NOT NULL,
            count_is_error BIGINT NOT NULL,
            UNIQUE (event_time, action)
        );
        ''')
        connection.commit()
        return connection
