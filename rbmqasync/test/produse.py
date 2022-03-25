import asyncio
import json
import sys
from os import environ
from pathlib import Path

from aio_pika import ExchangeType, Message
from aio_pika.abc import AbstractIncomingMessage
from logsmal import logger

from rbmqasync.test.consume import server_exchange, client_exchange

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from helpful import readAndSetEnv

readAndSetEnv("./devops/__env.env")
from rbmqasync.base_rbmq import RabbitmqAsync

RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"


@RabbitmqAsync.Connect(RABBITMQ_URL)
@RabbitmqAsync.Exchange(
    # Обменник для ответов серверов, будем использовать ``routing_key`` для направления ответа
    # в нужную очередь клиента, поэтому используем тип ``DIRECT``.
    name=server_exchange,
    type_=ExchangeType.DIRECT
)
@RabbitmqAsync.Queue(
    # Одна очередь для нескольких одинаковых серверов, которая ожидает сообщений от клиентов.
    name='server_queue',
    exclusive=True,
    # Так как обменник у клиентов типа ``FANOUT``, то путь будет игнорироваться, поэтому он пустой.
    bind={client_exchange: ('',)}
)
async def server(rabbitmq: RabbitmqAsync):
    async def get_message(message: AbstractIncomingMessage):
        # Если сообщение некуда возвращать, то игнорируем его
        if message.reply_to is not None:
            # Обрабатываем полученное сообщение
            async with message.process():
                data = json.loads(message.body.decode())
                logger.success(f"{message.correlation_id}|{data['data']}", 'GET MESSAGE')

                # Работаем
                response = str(eval(data['data']))

                # Отправляем ответ
                await rabbitmq.exchange[0].publish(
                    Message(
                        body=response.encode(),
                        correlation_id=message.correlation_id,
                    ),
                    routing_key=message.reply_to,
                )
                logger.success(message.reply_to, 'SEND MESSAGE')
        else:
            logger.error(f"{message.reply_to=}", "REPLAY TO")

    logger.info("Start", 'SERVER')
    # Ожидаем сообщений, как только получим его то, выловится функцию
    await rabbitmq.queue[0].consume(get_message)
    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(server())
