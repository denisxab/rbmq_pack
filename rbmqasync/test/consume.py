import asyncio
import datetime
import json
import sys
import uuid
from os import environ
from pathlib import Path

from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage
from logsmal import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from helpful import readAndSetEnv

readAndSetEnv("./devops/__env.env")
from rbmqasync.base_rbmq import RabbitmqAsync

RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"

server_exchange = 'sv_exchange'
client_exchange = 'cl_exchange'


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
    bind={server_exchange: (str(uuid.uuid4()),)}
)
async def client(rabbitmq: RabbitmqAsync):
    async def get_message(message: AbstractIncomingMessage):
        """
        Получить сообщение
        """
        # Проверяем id полученного сообщения с тем, что было отправлено на сервер.
        if message.correlation_id == message_id:
            # Обрабатываем полученное сообщение
            async with message.process():
                message_str = message.body.decode('utf-8')
                logger.success(f"{message.correlation_id=}|{message_str=}", "GET_MESSAGE")
        else:
            logger.error(f"{message.correlation_id}", 'CORONET ID')

    # Создаем id сообщения.
    message_id = str(uuid.uuid4())
    # Сериализуем данные.
    message = json.dumps({'time': str(datetime.datetime.now()), 'data': '10+5'})
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
    logger.success(f"{message_id}", "SEND MESSAGE")

    # Ожидаем сообщений, как только получим его то, выловится функцию
    await rabbitmq.queue[0].consume(get_message)
    logger.info("Start Consume", "CLIENT")
    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(client())
