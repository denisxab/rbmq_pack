from asyncio import Future
from json import dumps
from typing import Callable, Protocol
from uuid import uuid4

from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage
from logsmal import logger

from rbmqasync.rbmq import RabbitmqAsync


class CallbackGetMessage(Protocol):
    async def __call__(self, message: Message) -> None:
        """
        :param message: aio_pika.Message
        """
        ...


class CallbackPublish(Protocol):
    async def __call__(self, message_json: object, callback_get_message: Callable) -> None:
        """
        :param message_json: Объект сериализуемый в JSON
        :param callback_get_message: Функция которая вызовется при получение ответа на это сообщение
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
    async def web_js(publish: CallbackPublish):
        async def pr1(message: Message):
            logger.info(message.body, 'Web Js 1')

        async def pr2(message: Message):
            logger.info(message.body, 'Web Js 2')

        # Отправляем сообщение, и функцию которая вызовется когда мы полуем ответ на это сообщение
        await publish({'time': str(datetime.now()), 'data': '5+5'}, pr1)
        # Отправляем сообщение, и функцию которая вызовется когда мы полуем ответ на это сообщение
        await publish({'time': str(datetime.now()), 'data': '2+2'}, pr2)


    if __name__ == '__main__':
        asyncio.run(web_js())
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
            async def client_(rabbitmq: RabbitmqAsync):
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
                            logger.success(f"{message.correlation_id=}", "GET_MESSAGE")
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
                    logger.success(f"{message_id=}|{message=}", "SEND MESSAGE")

                # Ожидаем сообщений, как только получим его то, выловится функцию
                await rabbitmq.queue[0].consume(_get_message)
                logger.info("Start Consume", "CLIENT")
                # Вызываем функцию
                await func(*args, publish=_publish, **kwargs)
                # Запускаем вечный цикл
                await Future()

            await client_()

        return wraper

    return innser
