import argparse
from typing import Generator
from time import sleep
from random import randint, choice
from datetime import datetime

from kafka.producer.future import FutureRecordMetadata

from app.Resources import Resources
from app.Utils import get_app_logger, log_performance

logger = get_app_logger(__name__)


class EventGenerator:

    def __init__(self, resources: Resources):
        """
        Init EventGenerator resources with Dependency Injection pattern.
        """
        # Set args
        self.__min_delay_ms, self.__max_delay_ms = EventGenerator.parse_arguments()

        # DI
        self.__resources = resources
        # Init privates
        self.__actions = ['index', 'contact', 'search', 'login', 'logout', 'profile', 'buy', 'checkout', 'pay']

    @staticmethod
    def parse_arguments():
        """
        A static function to parse CLI arguments.
        :return:
        """
        ap = argparse.ArgumentParser()
        ap.add_argument("-m", "--min_delay_ms", required=True, type=int, choices=range(0, 1001),
                        metavar="[0-1000]", help="minimum delay between events in ms")
        ap.add_argument("-M", "--max_delay_ms", required=True, type=int, choices=range(0, 1001),
                        metavar="[0-1000]", help="maximum delay between events in ms")
        args = vars(ap.parse_args())
        return args['min_delay_ms'], args['max_delay_ms']

    @log_performance
    def run(self):
        """
        Generate events and send them into Kafka Service.
        :return: None
        """
        logger.info('Start sending events with params: {}'.format(
            (self.__min_delay_ms, self.__max_delay_ms)
        ))

        for _ in self.send_events():
            pass

        logger.debug('Block until all async messages are sent.')
        self.__resources.get_kafka_producer().flush()

    def send_events(self) -> Generator[
        FutureRecordMetadata, None, None
    ]:
        """
        Generator method, every next call sends a new event and does delay.
        :return: None
        """
        i = 0
        while True:
            # Dodanie nowego zdarzenia przy użyciu generatora.

            payload = {
                'request_end_time_minute': datetime.now().strftime('%m-%d-%Y %H:%M:0'),
                'action': choice(self.__actions),
                'duration_ms': randint(10, 3000),
                'is_error': choice([True, None, None, None]),
            }

            yield self.__resources.get_kafka_producer().send(Resources.TOPIC_RAW_DATA, payload)

            sleep_delay_s = randint(self.__min_delay_ms, self.__max_delay_ms) / 1000
            i += 1
            logger.debug('{} events are sent. Sleep delay: {}s. Payload: {}'.format(i, sleep_delay_s, payload))
            # Przerwa między dodawaniem zdarzeń.
            sleep(sleep_delay_s)

            # if i > 10: break


if __name__ == '__main__':
    logger.info('START EventGenerator')
    EventGenerator(
        Resources()
    ).run()
    logger.info('END EventGenerator')
