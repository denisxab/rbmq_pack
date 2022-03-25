import asyncio
import json
import sys
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
@RabbitmqAsync.Exchange(name='server_exchange', type_=ExchangeType.FANOUT)
@RabbitmqAsync.Queue(name='get_server', exclusive=True, bind={'client_exchange': ("",)})
async def server(rabbitmq: RabbitmqAsync):
    # async def get_message(message: AbstractIncomingMessage):
    #     """
    #     Получить сообщение
    #     """
    #     async with message.process(requeue=False):
    #         message_str = message.body.decode('utf-8')
    #         logger.success(f"{message.message_id}|{message_str}|{message.reply_to=}", "GET_MESSAGE")
    #
    #         response = input(":::")
    #         await rabbitmq.exchange['server_exchange'].publish(
    #             Message(
    #                 body=response.encode(),
    #                 correlation_id=message.correlation_id,
    #             ),
    #             routing_key=message.reply_to,
    #         )
    #         logger.success(f"{message.message_id}", "SEND_SERVER_MESSAGE")

    logger.info("Start", 'SERVER')
    async with rabbitmq.queue['get_server'].iterator() as qiterator:
        message: AbstractIncomingMessage
        async for message in qiterator:
            # Если сообщение некуда возвращать, то игнорируем его
            if message.reply_to is not None:
                async with message.process():
                    # Обрабатываем полученное сообщение
                    data = json.loads(message.body.decode())
                    logger.success(data['data'], 'GET MESSAGE')

                    # Вычитываем
                    response = str(eval(data['data']))

                    # Отправляем ответ
                    await rabbitmq.exchange['server_exchange'].publish(
                        Message(
                            body=response.encode(),
                            correlation_id=message.correlation_id,
                        ),
                        routing_key=message.reply_to,
                    )
                    logger.success(message.reply_to, 'SEND MESSAGE')
            else:
                logger.error(f"{message.reply_to=}", "REPLAY TO")


if __name__ == '__main__':
    asyncio.run(server())
