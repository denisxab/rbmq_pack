import asyncio
from random import randint
from datetime import datetime
from os import environ

from aio_pika import Message
from logsmal import logger

from helpful import readAndSetEnv

readAndSetEnv("./devops/__env.env")

from rbmqasync.rpc_rbmq import RPCClient, CallbackPublish

RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"


@RPCClient(
    server_exchange='sv_exchange',
    client_exchange='cl_exchange',
    RABBITMQ_URL=RABBITMQ_URL,
)
async def client_front(publish: CallbackPublish) -> None:
    async def pr1(message: Message):
        logger.info(message.body, 'Web Js 1')

    async def pr2(message: Message):
        logger.info(message.body, 'Web Js 2')

    await publish({'time': str(datetime.now()), 'data': f'{randint(0, 10)}+100'}, pr1)
    await publish({'time': str(datetime.now()), 'data': f'{randint(0, 10)}+10'}, pr2)


if __name__ == '__main__':
    asyncio.run(client_front())
