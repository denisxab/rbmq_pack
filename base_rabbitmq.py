from functools import partial
from os import environ
from typing import Callable, Union, Optional, Final

from logsmal import logger
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType
logger.rabbitmq_info = logger.info

#: Параметры подключения к ``Rabbitmq``
RABBITMQ_PARAMS: Final[ConnectionParameters] = ConnectionParameters(
    host=environ['RABBITMQ_IP'],
    # Аутентификация
    credentials=PlainCredentials(
        environ['RABBITMQ_DEFAULT_USER'],
        environ['RABBITMQ_DEFAULT_PASS']
    ),
    # Путь к виртуальному хосту
    virtual_host=environ['RABBITMQ_DEFAULT_VHOST']
)


class Rabbitmq:
    """
    Класс с декораторами
    """

    __slots__ = [
        "chanel",
        "exchange",
        "queue",
        "routing_key",
    ]

    def __init__(
            self,
            rabbitmq_chanel: Optional[BlockingChannel] = None,
            exchange: list[str] = None,
            queue: list[str] = None,
            routing_key: dict[str, str] = None,
    ):
        #: Канал ``Rabbitmq``
        self.chanel = rabbitmq_chanel
        #: Отправители
        self.exchange = [] if exchange is None else exchange
        #: Список очередей
        self.queue = [] if queue is None else queue
        #: Пути отслеживания
        self.routing_key = dict() if routing_key is None else routing_key

    def queue_delete(self):
        """Удалить все очереди"""
        for _x in self.queue:
            self.chanel.queue_delete(_x)

    @staticmethod
    def connect(
            channel_number: int = 1,
            rabbitmq_params: ConnectionParameters = RABBITMQ_PARAMS
    ):
        """
        Декоратор для подключения к ``Rabbitmq``

        :param exchange: Имя очереди к который нужно подключиться
        :param rabbitmq_params: Данные для подключения к ``Rabbitmq``
        :param channel_number: Цифра канала к которому подключиться
        """

        def inner(func: Callable):
            def warp(*arg, **kwargs):
                # Подключиться к ``Rabbitmq``
                rabbitmq_cli = BlockingConnection(rabbitmq_params)
                # Подключиться к каналу
                with rabbitmq_cli.channel(channel_number=channel_number) as rabbitmq_chanel:
                    rabbitmq: Rabbitmq = Rabbitmq(rabbitmq_chanel=rabbitmq_chanel)
                    logger.rabbitmq_info(f"{channel_number=}", flag='CREATE_CHANEL')
                    func(
                        *arg,
                        rabbitmq=rabbitmq,
                        # Имя очереди
                        **kwargs
                    )

            return warp

        return inner

    @staticmethod
    def Exchange(
            exchange: str = '',
            exchange_type: ExchangeType = ExchangeType.fanout
    ):
        """
        Подключиться к разветвленной очереди

        :param exchange:
        :param exchange_type:

            https://www.rabbitmq.com/tutorials/amqp-concepts.html

            - fanout - Одно и то же сообщение во все очереди
            - direct - Одно и то же сообщение в разные пути
            - topic - Одно и то же сообщение в разные пути на основе шаблона.

                Отправитель имеет конкретное имя темы например `user.guid.login`

                А получатели должны указать какой то шаблон который он будет принимать,
                например только темы которые заканчиваются на  `#.login`


                - Текст целиком ``routing_key=<тема>.<тема>.<тема>...``
                - * (star) может заменить ровно одно слово - ``routing_key="*.<тема>"``
                - # (hash) может заменить ноль или более слов - ``routing_key="<тема>.#"``

                .. note

                    !!! Слова должны быть разделены точками пример

        .. code-block:: python

            @connectRabbitmq()
            @connectExchangeRabbitmq
            def ...():
                ...
        """

        def inner(func: Callable):
            def warp(*args, rabbitmq: Rabbitmq, **kwargs):
                #: Подключиться к разветвленной очереди
                rabbitmq.chanel.exchange_declare(exchange=exchange, exchange_type=exchange_type.name)
                logger.rabbitmq_info(f"{exchange=}:{exchange_type=}", flag='CREATE_EXCHANGE')
                # Добавляем имя ``exchange`` в список
                rabbitmq.exchange.append(exchange)
                # Вызываем функцию
                func(*args, rabbitmq=rabbitmq, **kwargs)

            return warp

        return inner

    @staticmethod
    def Queue(
            queue: str = '',
            routing_key: tuple[Union[str, None], ...] = None,
            prefetch_count: int = 0,
            prefetch_size: int = 0,
            random_exclusive_queue: bool = False,
    ):
        """
        Создать случайную очередь уникальную для получателя

        :param queue:
        :param random_exclusive_queue: Если ``True`` у очереди будет случайное уникально имя
        :param prefetch_size: Максимальный размер очереди
        :param prefetch_count: Максимальное количество сообщений в очереди
        :param routing_key: Пути отлеживаемые в очереди

        .. code-block:: python

            @connectRabbitmq()
            @connectExchangeRabbitmq
            @createRandomQueue
            def ...():
                ...
        """

        def inner(func: Callable):

            def warp(*args, rabbitmq: Rabbitmq, **kwargs):

                def createQueue() -> str:
                    """Создать случайную уникальную очередь"""
                    if random_exclusive_queue:
                        return rabbitmq.chanel.queue_declare(
                            # '' - означает взять случайное уникальное имя очереди
                            queue="",
                            # После того как соединение будет закрыта очередь удалиться
                            exclusive=True
                        ).method.queue
                    else:
                        return rabbitmq.chanel.queue_declare(
                            queue=queue,
                        ).method.queue

                queue_name: str = createQueue()
                logger.rabbitmq_info(f"{queue_name=}", flag="CREATE_QUEUE")

                # Добавляем имя очередь в список
                rabbitmq.queue.append(queue_name)
                # Добавляем имена путей в очереди
                if isinstance(rabbitmq.routing_key, dict):
                    rabbitmq.routing_key[queue_name] = routing_key

                #: Количество/размер сообщений, которые могут стоять в очереди для этого получателя.
                #: Если превысить эти значения, то сообщения отправятся другому свободному получателю.
                rabbitmq.chanel.basic_qos(prefetch_count=prefetch_count, prefetch_size=prefetch_size)

                #: Привязать пути прослушивания если они есть
                if routing_key:
                    #: Привязываем отслеживание маршрутов к этой очереди
                    for _rk in routing_key:
                        # Подключиться к очереди
                        rabbitmq.chanel.queue_bind(
                            exchange=rabbitmq.exchange[0],
                            queue=queue_name,
                            routing_key=_rk
                        )
                        logger.rabbitmq_info(f"{queue_name=}:{_rk=}", flag="CREATE_ROUTE")

                #: Вызываем функцию
                func(*args, rabbitmq=rabbitmq, **kwargs)

            return warp

        return inner

    def call_publish(
            self,
            properties: Union[Callable, object, None] = None,
            mandatory=False,
            routing_key=None,
            exchange=None,
    ) -> Callable[[bytes], None]:
        """
        Готовая функция для ``basic_publish`` в которую нужно отправить только ``body``

        :param properties: Можно передать функцию которая
            будет выполняться при каждой публикации
        :param exchange:
        :param routing_key:
        :param mandatory:
        :param rabbitmq_chanel:

        ..code-block:: python

           call_publish = Rabbitmq.call_publish(
                properties= lambda: pika.BasicProperties(
                    reply_to=queue[0],
                    correlation_id=str(uuid.uuid4()),
                ),
                rabbitmq_chanel=rabbitmq_chanel,
                exchange=exchange,
                **rewrite_key(kwargs, routing_key=queue[1])
            )
            call_publish(body="Сообщение".encode())

        """

        def self_(
                body, *,
                exchange,
                routing_key,
                properties,
                mandatory,
        ):
            self.chanel.basic_publish(
                exchange,
                routing_key,
                body,
                # Если это функция, то вызываем её
                properties() if callable(properties) else properties,
                mandatory,
            )

        return partial(
            self_,
            exchange=exchange if exchange is not None else self.exchange[0],
            mandatory=mandatory,
            # Если значение переопределено, то берем переопределенное значение
            routing_key=routing_key if routing_key else self.routing_key,
            properties=properties
        )


class EventRabbit:

    def __init__(self):
        self.event = False

    def set(self):
        self.event = True

    def clear(self):
        self.event = False
