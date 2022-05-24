from app.Resources import Resources
from app.Utils import log_performance, get_app_logger

logger = get_app_logger(__name__)


class EventReceiver:
    """
    EventReceiver specializes in consuming messages from one kafka topic and batch save them into storage.
    """

    def __init__(self, resources: Resources):
        """
        Init EventReceiver resources.
        """
        self.__resources = resources

    @log_performance
    def run(self):
        """
        Eternal loop for consume computed payloads from Kafka, then save them in storage.
        """
        while True:
            try:
                payloads = self.__resources.get_kafka_consumer()  # type: dict

                connection = self.__resources.get_storage()
                cursor = connection.cursor()

                for payload in payloads:
                    logger.debug('Received message: {}'.format(payload.value))

                    # Upsert computed stats from PySpark
                    cursor.execute('''
                        INSERT INTO public.computed_data (event_time, action, avg_duration_ms, count, count_is_error)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_time, action) 
                        DO UPDATE SET 
                            avg_duration_ms = EXCLUDED.avg_duration_ms, 
                            count = EXCLUDED.count, 
                            count_is_error = EXCLUDED.count_is_error
                    ''', (
                        payload.value['eventTime'], payload.value['action'], payload.value['avg_duration_ms'],
                        payload.value['count'], payload.value['count_is_error']
                    )
                                   )

                    connection.commit()

                    # .computed_data.update_one(
                    #     {
                    #         'eventTime': payload.value['eventTime'],
                    #         'action': payload.value['action']
                    #     },
                    #     {'$set': payload.value},
                    #     upsert=True
                    # )
            except Exception as e:
                logger.error(e)


if __name__ == '__main__':
    logger.info('START EventReceiver')
    EventReceiver(
        Resources()
    ).run()
    logger.info('END EventReceiver')
