import asyncio
from os import environ
from typing import Union

from logsmal import logger

from helpful import readAndSetEnv

readAndSetEnv("./devops/__env.env")

from rbmqasync.client_server import RPCServer

RABBITMQ_URL: str = f"amqp://{environ['RABBITMQ_DEFAULT_USER']}:{environ['RABBITMQ_DEFAULT_PASS']}@{environ['RABBITMQ_IP']}{environ['RABBITMQ_DEFAULT_VHOST']}"


@RPCServer(
    server_exchange='sv_exchange',
    client_exchange='cl_exchange',
    RABBITMQ_URL=RABBITMQ_URL,
    name_sever_queue='server_queue'
)
async def server_back(data: Union[object, dict, list]) -> str:
    logger.success(data, "GET DATA")

    return str(eval(data['data']))


if __name__ == '__main__':
    asyncio.run(server_back())
