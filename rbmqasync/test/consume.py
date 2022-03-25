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
from rbmqasync.rbmq import RabbitmqAsync

exchange_name = 'test_exchange'
RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"


@RabbitmqAsync.Connect(RABBITMQ_URL)
@RabbitmqAsync.Exchange(name='client_exchange', type_=ExchangeType.FANOUT)
@RabbitmqAsync.Queue(name='get_client',
                     exclusive=True,
                     bind={'server_exchange': ("",)})
async def client(rabbitmq: RabbitmqAsync):
    async def get_message(message: AbstractIncomingMessage):
        """
        Получить сообщение
        """
        # У сообщения должно быть id
        if message.correlation_id is not None:
            # Обрабатываем полученное сообщение
            async with message.process():
                message_str = message.body.decode('utf-8')
                logger.success(f"{message.reply_to=}|{message_str=}", "GET_MESSAGE")
                # response = input(":::")
                # await rabbitmq.chanel.default_exchange.publish(
                #     Message(
                #         body=response.encode(),
                #         correlation_id=message.correlation_id,
                #         reply_to='get_client',
                #     ),
                #     routing_key=message.reply_to,
                # )
                # logger.success(f"{message.message_id}", "SEND_SERVER_MESSAGE")
        else:
            logger.error(f"{message}", 'CORONET ID')

    message = json.dumps({'time': str(datetime.datetime.now()), 'data': '10+5'})

    # Настраиваем ожидание сообщений, как только получим сообщение то, вызовем функцию
    await rabbitmq.queue['get_client'].consume(get_message)
    logger.info("Settings", "CLIENT")

    # Отправляем сообщение серверу, с указанием id сообщения(correlation_id),
    # и имя очереди в которое нужно вернуть ответ(reply_to).
    await rabbitmq.exchange['client_exchange'].publish(
        message=Message(
            message.encode(),
            content_type="text/plain",
            reply_to='get_server',
            correlation_id=str(uuid.uuid4()),
        ),
        routing_key='rq',
    )
    logger.success("SEND MESSAGE ", "CLIENT")

    # Запускам вечный цикл
    logger.info("Start Consume", "CLIENT")
    await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(client())
