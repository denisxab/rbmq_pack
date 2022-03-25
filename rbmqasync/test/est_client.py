import asyncio
from datetime import datetime
from os import environ
from typing import Callable

from aio_pika import Message
from logsmal import logger

from helpful import readAndSetEnv

readAndSetEnv("./devops/__env.env")

from rbmqasync.client_server import Client

RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"


@Client(
    server_exchange='sv_exchange',
    client_exchange='cl_exchange',
    RABBITMQ_URL=RABBITMQ_URL,
)
async def web_js(publish: Callable):
    async def pr1(message: Message):
        logger.info(message.body, 'Web Js 1')

    async def pr2(message: Message):
        logger.info(message.body, 'Web Js 2')

    await publish({'time': str(datetime.now()), 'data': '5+5'}, pr1)
    await publish({'time': str(datetime.now()), 'data': '2+2'}, pr2)


if __name__ == '__main__':
    asyncio.run(web_js())
