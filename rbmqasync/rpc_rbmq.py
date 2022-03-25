from asyncio import Future
from json import dumps, loads
from typing import Callable, Protocol, Union
from uuid import uuid4

from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage

from rbmqasync.base_rbmq import RabbitmqAsync, logger


class CallbackGetMessage(Protocol):
    async def __call__(self, message: Message) -> None:
        """
        Эта функция вызывается в ``async with message.process(): ...``

        :param message: aio_pika.Message
        """
        ...


class CallbackRPCServer(Protocol):
    async def __call__(self, data: Union[object, dict, list]) -> str:
        """
        Тип для оборачиваемой функции RPCServe
        """
        ...


class CallbackPublish(Protocol):
    async def __call__(self, message_json: object, callback_get_message: CallbackGetMessage) -> None:
        """
        функция для отправки данных на сервер

        :param message_json: Объект сериализуемый в JSON
        :param callback_get_message: Функция которая вызовется при получение ответа на это сообщение
        """
        ...


class CallbackRPCClient(Protocol):
    async def __call__(self, publish: CallbackPublish) -> None:
        """
        Тип для оборачиваемой функции RPCClient
        """
        ...


def RPCClient(
        server_exchange: str,
        client_exchange: str,
        RABBITMQ_URL: str,
):
    """
    Декоратор для создания RPC клиента. Смысл в том чтобы выполнять трудоемкие задачи на стороне сервера
    а клиент получит только готовый ответ.

    :param server_exchange: Точка обмена для серверов (должна быть заранее создана)
    :param client_exchange: Точка обмена для клиентов (если нет то создастся)
    :param RABBITMQ_URL: Url подключения к ``Rabbitmq``
    :return: Функция для отправки сообщений на сервер ``publish``

    :Пример:

    RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"

    @RPCClient(
        server_exchange='sv_exchange',
        client_exchange='cl_exchange',
        RABBITMQ_URL=RABBITMQ_URL,
    )
    async def client_front(publish: CallbackPublish):
        async def pr1(message: Message):
            logger.rabbitmq_info (message.body, 'Web Js 1')

        async def pr2(message: Message):
            logger.rabbitmq_info (message.body, 'Web Js 2')

        # Отправляем сообщение, и функцию которая вызовется когда мы полуем ответ на это сообщение
        await publish({'time': str(datetime.now()), 'data': '5+5'}, pr1)
        # Отправляем сообщение, и функцию которая вызовется когда мы полуем ответ на это сообщение
        await publish({'time': str(datetime.now()), 'data': '2+2'}, pr2)


    if __name__ == '__main__':
        asyncio.run(client_front())
    """

    def innser(func):
        async def wraper(*args, **kwargs):
            @RabbitmqAsync.Connect(RABBITMQ_URL)
            @RabbitmqAsync.Exchange(
                # Подключаемся к общему обменнику для клиентов.
                name=client_exchange,
                # Так как у нас одна очередь для серверов, то можно не учитывать пути
                type_=ExchangeType.FANOUT
            )
            @RabbitmqAsync.Queue(
                # Создаем уникальную очередь для клиента
                name='',
                exclusive=True,
                # Подключаем её к точке обмена, указываем уникальный путь,
                # в который сервер будут отправлять ответ для клиента.
                bind={server_exchange: (str(uuid4()),)}
            )
            async def _client(rabbitmq: RabbitmqAsync):
                # Словарь для функций, ключ это id сообщения, значения это функция которая
                # вызовется при получении сообщения с `correlation_id == message_id`.
                dict_callback: dict[str, Callable] = {}

                async def _get_message(message: AbstractIncomingMessage):
                    """
                    Получить сообщение
                    """
                    # У сообщения должно быть id
                    if message.correlation_id is not None:
                        # Обрабатываем полученное сообщение, подтверждения будет поле выхода из контекста
                        async with message.process():
                            logger.rabbitmq_success (f"{message.correlation_id=}", "GET_MESSAGE")
                            # Вызываем функцию с таким id сообщения
                            await dict_callback[message.correlation_id](message)
                    else:
                        logger.error(f"{message.correlation_id}", 'CORONET ID')

                async def _publish(message_json: object, callback: Callable):
                    # Создаем id сообщения.
                    message_id = str(uuid4())
                    # Добавляем ``callback`` для id сообщения
                    dict_callback[message_id] = callback
                    # Сериализуем данные.
                    message = dumps(message_json)
                    # Отправляем сообщение серверу.
                    await rabbitmq.exchange[0].publish(
                        message=Message(
                            message.encode(),
                            content_type="application/json",
                            # Задаем id для сообщения.
                            correlation_id=message_id,
                            # Ответ должен быть направлен в уникальный `routing_key` клиента.
                            reply_to=rabbitmq.routing_key[server_exchange][0],
                        ),
                        # Так как тип ``FANOUT`` нам неважен путь, он все равно про игнорируется.
                        routing_key='',
                    )
                    logger.rabbitmq_success (f"{message_id=}|{message=}", "SEND MESSAGE")

                # Ожидаем сообщений, как только получим его то, выловится функцию
                await rabbitmq.queue[0].consume(_get_message)
                logger.rabbitmq_info("Start Consume", "CLIENT")
                # Вызываем функцию
                await func(*args, publish=_publish, **kwargs)
                # Запускаем вечный цикл
                await Future()

            await _client()
            # Оборачиваемая функция должна соответствовать типу ``CallbackRPCServer``

        if func.__annotations__ != CallbackRPCClient.__call__.__annotations__:
            raise AttributeError(f"Неверный тип функции, должен быть {CallbackRPCClient.__call__.__annotations__}")

        return wraper

    return innser


def RPCServer(
        server_exchange: str,
        client_exchange: str,
        RABBITMQ_URL: str,
        name_sever_queue='server_queue',
        prefetch_count=10,
        prefetch_size=0,
):
    """
    :param server_exchange: Точка обмена для серверов (если нет то создастся)
    :param client_exchange: Точка обмена для клиентов  (должна быть заранее создана)
    :param RABBITMQ_URL: Url подключения к ``Rabbitmq``
    :param name_sever_queue: Имя для общей очереди у серверов
    :param prefetch_count: Максимальный размер очереди сообщений для одного канала
    :param prefetch_size: Максимально количество сообщений в очереди для одного канала
    :return: Оборачиваемая функция будет вызваться при получении сообщения

    :Пример:

    RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"


    @RPCServer(
        server_exchange='sv_exchange',
        client_exchange='cl_exchange',
        RABBITMQ_URL=RABBITMQ_URL,
        name_sever_queue='server_queue'
    )
    async def server_back(data: Union[object, dict, list]) -> str:
        return str(eval(data['data']))

    if __name__ == '__main__':
        asyncio.run(server_back())

    """

    def innser(func: CallbackRPCServer):
        async def wraper(*args, **kwargs):

            @RabbitmqAsync.Connect(
                RABBITMQ_URL,
                prefetch_count=prefetch_count,
                prefetch_size=prefetch_size,
            )
            @RabbitmqAsync.Exchange(
                # Обменник для ответов серверов, будем использовать ``routing_key`` для направления ответа
                # в нужную очередь клиента, поэтому используем тип ``DIRECT``.
                name=server_exchange,
                type_=ExchangeType.DIRECT
            )
            @RabbitmqAsync.Queue(
                # Одна очередь для нескольких одинаковых серверов, которая ожидает сообщений от клиентов.
                name=name_sever_queue,
                exclusive=True,
                # Так как обменник у клиентов типа ``FANOUT``, то путь будет игнорироваться, поэтому он пустой.
                bind={client_exchange: ('',)}
            )
            async def _server(rabbitmq: RabbitmqAsync):
                async def _get_message(message: AbstractIncomingMessage):
                    # Если сообщение некуда возвращать, то игнорируем его
                    if message.reply_to is not None:
                        # Обрабатываем полученное сообщение
                        async with message.process():
                            # Десиреализуем данные
                            data = loads(message.body.decode())
                            logger.rabbitmq_success (f"{message.correlation_id}|{data['data']}", 'GET MESSAGE')
                            # Выполняем полезную нагрузку
                            response: str = await func(data)
                            # Отправляем ответ
                            await rabbitmq.exchange[0].publish(
                                Message(
                                    body=response.encode(),
                                    correlation_id=message.correlation_id,
                                ),
                                routing_key=message.reply_to,
                            )
                            logger.rabbitmq_success (message.reply_to, 'SEND MESSAGE')
                    else:
                        logger.error(f"{message.reply_to=}", "REPLAY TO")

                logger.rabbitmq_info("Start", 'SERVER')
                # Ожидаем сообщений, как только получим его то, выловится функцию
                await rabbitmq.queue[0].consume(_get_message)
                # Вечный цикл
                await Future()

            await _server()

        # Оборачиваемая функция должна соответствовать типу ``CallbackRPCServer``
        if func.__annotations__ != CallbackRPCServer.__call__.__annotations__:
            raise AttributeError(f"Неверный тип функции, должен быть {CallbackRPCServer.__call__.__annotations__}")

        return wraper

    return innser
