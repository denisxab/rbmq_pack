import asyncio
import sys
import uuid
from asyncio import run
from datetime import datetime
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


class te_topic:
    """
    Проверка работы отправки одно и того же сообщения в разные очереди
    на основе тем.
    """

    @staticmethod
    @RabbitmqAsync.Connect(RABBITMQ_URL)
    @RabbitmqAsync.Exchange(name=exchange_name, type_=ExchangeType.TOPIC)
    @RabbitmqAsync.Queue(name='', bind={exchange_name: ("*.git",)}, exclusive=True)
    async def consumer_echange_topic1(rabbitmq: RabbitmqAsync):
        logger.info("consumer_1", "START")
        await rabbitmq.start_consume('0', rabbitmq.get_message)

    @staticmethod
    @RabbitmqAsync.Connect(RABBITMQ_URL)
    @RabbitmqAsync.Exchange(name=exchange_name, type_=ExchangeType.TOPIC)
    @RabbitmqAsync.Queue(name='qwee', bind={exchange_name: ("user.#",)}, exclusive=True)
    async def consumer_echange_topic2(rabbitmq: RabbitmqAsync):
        logger.info("consumer_2", "START")
        await rabbitmq.start_consume('qwee', rabbitmq.get_message)

    @staticmethod
    @RabbitmqAsync.Connect(RABBITMQ_URL)
    @RabbitmqAsync.Exchange(name=exchange_name, type_=ExchangeType.TOPIC)
    async def producer_echange_topic(rabbitmq: RabbitmqAsync):
        logger.info("producer", "START")
        await rabbitmq.publish(exchange_name, routing_key=("user.git",),
                               message=f"Hello {datetime.now()}")





        # logger.info("Consume client")
        # Указываем логику работы с полученным сообщением от сервера
        # await rabbitmq.start_consume(queue_name='get_client', callback_=get_message)
        # Указываем логику работы с полученным сообщением от сервера
        # rabbitmq.chanel.basic_consume(
        #     # Случайная уникальная очередь
        #     queue=rabbitmq.queue[0],
        #     # Что делать с полученным сообщением
        #     on_message_callback=partial(
        #         getMessageSuccess,
        #         callback=self_
        #     )
        # )
        #
        # # Готовая функция для ``basic_publish`` в которую нужно отправить только ``body``
        # call_publish = rabbitmq.call_publish(
        #     # Указать обратную связь
        #     properties=lambda: pika.BasicProperties(
        #         # В какую очередь вернуть ответ
        #         reply_to=rabbitmq.queue[0],
        #         # ID сообщения
        #         correlation_id=str(uuid4()),
        #     ),
        #     routing_key=rabbitmq.queue[1],
        #     exchange=''
        # )


if __name__ == '__main__':
    argv_ = '1'  # sys.argv[1]
    if argv_ == '1':
        run(te_clint_server.RPCClient())
        # run(te_topic.consumer_echange_topic1())
    elif argv_ == '2':
        run(te_clint_server.server())
        # run(te_topic.consumer_echange_topic2())
    elif argv_ == '3':
        run(te_topic.producer_echange_topic())
