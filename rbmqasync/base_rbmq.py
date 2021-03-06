from asyncio import Future
from typing import Callable, Union, Optional
from typing import Protocol, Any

from aio_pika import Message
from aio_pika import connect_robust
from aio_pika.abc import AbstractRobustChannel, ExchangeType
from aio_pika.abc import AbstractRobustExchange, AbstractRobustQueue, AbstractIncomingMessage
from logsmal import logger

logger.rabbitmq_info = logger.info
logger.rabbitmq_info.title_logger = "RBMQ INFO"
logger.rabbitmq_success = logger.success
logger.rabbitmq_success.title_logger = "RBMQ SUCCESS"


class RabbitmqAsync:
    """
    Класс с асинхронными декораторами для ``Rabbitmq``

    https://aio-pika.readthedocs.io/en/latest/apidoc.html


    # Подтверждение сообщения

    https://aio-pika.readthedocs.io/en/latest/apidoc.html#aio_pika.IncomingMessage

    queue.consume()

    - `no_ack=True` - Автоматически подержать сообщение при получении.
    - `no_ack=False` - Отметить автоматическое подтверждение при получении сообщения.

    Подтверждать в этом случае нужно вручную:

    - await message.ack() - Потвердеть получение сообщения
    - async with message.process(): ... - подтвердит сообщение
        при выходе из контекста, а если контекстный процессор поймает исключение,
        сообщение будет возвращено в очередь.

    - await message.reject(True) - Отменить получение, и вернуть сообщение в очередь

    # Долговечность сообщения

    https://aio-pika.readthedocs.io/en/latest/rabbitmq-tutorial/2-work-queues.html#message-durability

    1) Создать устойчивую очередь

        @RabbitmqAsync.Queue(durable=True):...

    2) Отправлять устойчивые сообщения

        await publish(delivery_mode=DeliveryMode.PERSISTENT)


    # Клиент серверный (!!!)

    https://aio-pika.readthedocs.io/en/latest/rabbitmq-tutorial/6-rpc.html

    """

    __slots__ = [
        "chanel",
        "exchange",
        "queue",
        "routing_key",
    ]

    def __init__(
            self,
            chanel: Optional[AbstractRobustChannel] = None,
            exchange: list[AbstractRobustExchange] = None,
            queue: list[AbstractRobustQueue] = None,
            routing_key: dict[str, tuple[Union[str, None], ...]] = None,
    ):
        #: Канал ``Rabbitmq``
        self.chanel: AbstractRobustChannel = chanel
        #: Точки обмена
        self.exchange: list[AbstractRobustExchange] = [] if exchange is None else exchange
        #: Список очередей
        self.queue: list[AbstractRobustQueue] = [] if queue is None else queue
        #: Ключевые пути
        self.routing_key: dict[str, list[str]] = dict() if routing_key is None else routing_key

    async def publish(self,
                      exchange_index: int,
                      routing_key: tuple[str, ...],
                      message: str, delivery_mode=None,
                      reply_to: str = None,
                      correlation_id: str = None
                      ):
        """
        Отправить сообщение в точку обмена


        :param correlation_id:
        :param exchange_index: Имя точки обмена
        :param routing_key: Ключевые пути
        :param message: Сообщение
        :param reply_to: Имя очереди в которую вернется ответ
        :param delivery_mode:

            - DeliveryMode.PERSISTENT - Сохранить сообщение на диске
        """
        message: bytes = message.encode("utf-8")
        for _r in routing_key:
            logger.rabbitmq_success(f"{message=}|{exchange_index=}|{routing_key=}", "SEND_MESSAGE")
            await self.exchange[exchange_index].publish(
                message=Message(
                    body=message,
                    delivery_mode=delivery_mode,
                    reply_to=reply_to,
                    correlation_id=correlation_id,
                ),
                routing_key=_r
            )

    class CallbackConsume(Protocol):
        async def __call__(self, message: AbstractIncomingMessage, *args, **kwargs) -> Any: ...

    async def start_consume(self, queue_index: int, callback_: CallbackConsume):
        """
        Ожидать сообщения в бесконечном цикле

        :param queue_index: Имя очереди
        :param callback_: Функция вызовется при получении сообщения
        """
        logger.rabbitmq_info(f"{queue_index=}|{self.queue[queue_index]}", "CONSUME")

        await self.queue[queue_index].consume(callback=callback_,
                                              # Отключить авто подтверждение получения сообщения
                                              no_ack=False)
        # Вечный цикл
        await Future()

    @staticmethod
    async def get_message(message: AbstractIncomingMessage):
        """
        Получить сообщение
        """
        async with message.process():
            message_str = message.body.decode('utf-8')
            logger.rabbitmq_success(f"{message.message_id}|{message_str}", "GET_MESSAGE")

    @staticmethod
    def Connect(
            url: str,
            channel_number: int = 1,
            prefetch_count: int = 0,
            prefetch_size: int = 0,
            ssl=False,
            ssl_options=None,
    ):
        """
        Декоратор для подключения к ``Rabbitmq``

        :param ssl_options:
        :param ssl:
        :param url: Url для подключения к Rabbitmq.
        :param prefetch_count: Максимальное количество сообщений в очереди
        :param prefetch_size: Максимальный размер очереди

        :Пример:

        - "amqp://UserName:Password@127.0.0.1/"
        - f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"

        :param channel_number: Цифра канала к которому подключиться
        """

        def inner(func: Callable):
            async def warp(*arg, **kwargs):
                # Подключиться к ``Rabbitmq``
                connection = await connect_robust(url, ssl=ssl, ssl_options=ssl_options)
                # Подключиться к каналу
                async with connection:
                    rabbitmq: RabbitmqAsync = RabbitmqAsync(
                        chanel=await connection.channel(channel_number=channel_number)
                    )

                    #: Устанавливаем ограничения каналу для отправки сообщений в одну очередь
                    #: Количество/размер сообщений, которые могут стоять в очереди для этого получателя.
                    #: Если превысить эти значения, то сообщения отправятся другому свободному получателю.
                    await rabbitmq.chanel.set_qos(
                        # Макс количество сообщений
                        prefetch_count=prefetch_count,
                        # Макс размер очереди
                        prefetch_size=prefetch_size
                    )

                    logger.rabbitmq_info(f"{channel_number=}", flag='CREATE_CHANEL')
                    await func(
                        *arg,
                        rabbitmq=rabbitmq,
                        # Имя очереди
                        **kwargs
                    )

            return warp

        return inner

    @staticmethod
    def ConnectTransactions(
            url: str,
            channel_number: int = 1,
            prefetch_count: int = 0,
            prefetch_size: int = 0,
    ):
        """
        https://aio-pika.readthedocs.io/en/latest/quick-start.html#working-with-rabbitmq-transactions

        :param url:
        :param channel_number:
        :param prefetch_count:
        :param prefetch_size:
        :return:
        """

        def inner(func: Callable):
            async def warp(*arg, **kwargs):
                # Подключиться к ``Rabbitmq``
                connection = await connect_robust(url)
                # Подключиться к каналу
                async with connection:
                    rabbitmq: RabbitmqAsync = RabbitmqAsync(
                        chanel=await connection.channel(channel_number=channel_number, publisher_confirms=False)
                    )

                    async with rabbitmq.chanel.transaction():
                        #: Устанавливаем ограничения каналу для отправки сообщений в одну очередь
                        #: Количество/размер сообщений, которые могут стоять в очереди для этого получателя.
                        #: Если превысить эти значения, то сообщения отправятся другому свободному получателю.
                        await rabbitmq.chanel.set_qos(
                            # Макс количество сообщений
                            prefetch_count=prefetch_count,
                            # Макс размер очереди
                            prefetch_size=prefetch_size
                        )
                        logger.rabbitmq_info(f"{channel_number=}", flag='CREATE_CHANEL')
                        await func(
                            *arg,
                            rabbitmq=rabbitmq,
                            # Имя очереди
                            **kwargs
                        )

            return warp

        return inner

    @staticmethod
    def Exchange(
            name: str = '',
            type_: ExchangeType = ExchangeType.DIRECT
    ):
        """
        Подключиться к точке обмена

        :param name: Имя для точки обмена
        :param type_: Тип точки обмена

            - fanout -  КлючевыеПути не учитываются
            - direct - полное совпадение ПутевогоКлюча
            - topic - ПутевойКлюч удовлетворяет маске(шаблону),

                Про шаблон:

                - слова должны быть разделены через точку (`.`)
                - символ `*` - может заменить ровно одно слово - `"*.<тема>"`
                - символ `#` - может заменить ноль или более слов - `"<тема>.#"`

            https://www.rabbitmq.com/tutorials/amqp-concepts.html
        """

        def inner(func: Callable):
            async def warp(*args, rabbitmq: RabbitmqAsync, **kwargs):
                #: Подключиться к точке обмена
                rabbitmq.exchange.append(await rabbitmq.chanel.declare_exchange(name=name, type=type_.value))
                logger.rabbitmq_info(f"{name=}:{type_=}", flag='CREATE_EXCHANGE')
                await func(*args, rabbitmq=rabbitmq, **kwargs)

            return warp

        return inner

    @staticmethod
    def Queue(
            name: str,
            bind: Optional[dict[str, tuple[str, ...]]] = None,
            exclusive: bool = False,
            durable=False,
    ):
        """
        Создать очередь

        :param durable: Если ``True`` очередь будет устойчивой, это достигается путем сохранения ей на диске.
        :param name: Имя очереди, если '' то у очереди будет случайное уникальное имя
        :param bind: Связать очередь с точкой обмена -  {"ExchangeName": ("КлючевыеПути", ... ) ... }
        :param exclusive: Очередь будет удалена при закрытии соединения.
        В это случае ключ в ``rabbitmq.queue``, будет называться по индексу создания очереди
        """

        def inner(func: Callable):

            async def warp(*args, rabbitmq: RabbitmqAsync, **kwargs):

                # Создаем очередь
                queue_obj: AbstractRobustQueue = await rabbitmq.chanel.declare_queue(
                    # '' - означает взять случайное уникальное имя очереди
                    name=name,
                    # После того как соединение будет закрыта очередь удалиться
                    exclusive=exclusive,
                    durable=durable,
                )
                logger.rabbitmq_info(f"{queue_obj.name=}", flag="CREATE_QUEUE")

                #: Привязать ключевые пути если они есть
                if bind:
                    for _exchange, _routing_keys in bind.items():
                        # Дополняем имя точки обмена
                        rabbitmq.routing_key[_exchange]: list[str] = []
                        for _routing_k in _routing_keys:
                            await queue_obj.bind(
                                exchange=_exchange,
                                routing_key=_routing_k
                            )
                            # Добавляем ключевые пути к ключу точки обмена
                            rabbitmq.routing_key[_exchange].append(_routing_k)
                            logger.rabbitmq_info(f"{_exchange=}:{_routing_k=}:", flag="CREATE_ROUTE")

                rabbitmq.queue.append(queue_obj)
                await func(*args, rabbitmq=rabbitmq, **kwargs)

            return warp

        return inner


class UtilitiesRabbitmq:
    """
    Утилиты для Rabbitmq
    """

    @staticmethod
    def queue_delete(name: str, *, url: str, channel_number: int = 1):
        """Удалить очередь по имени"""

        @RabbitmqAsync.Connect(url=url, channel_number=channel_number)
        def _self(rabbitmq: RabbitmqAsync):
            rabbitmq.chanel.queue_delete(queue_name=name)

        _self()

    @staticmethod
    def exchange_delete(name: str, *, url: str, channel_number: int = 1):
        """Удалить точку обмена по имени"""

        @RabbitmqAsync.Connect(url=url, channel_number=channel_number)
        def _self(rabbitmq: RabbitmqAsync):
            rabbitmq.chanel.exchange_delete(exchange_name=name)

        _self()

    @staticmethod
    def queue_unbind(
            index: int,
            exchange_name: str, *,
            url: str,
            routing_key: str = None,
            channel_number: int = 1
    ):
        """Отвязать очередь от точки обмена"""

        @RabbitmqAsync.Connect(url=url, channel_number=channel_number)
        def _self(rabbitmq: RabbitmqAsync):
            rabbitmq.queue[index].unbind(exchange=exchange_name, routing_key=routing_key)

        _self()
