from asyncio import Future
from typing import Protocol, Any

from aio_pika import Message
from aio_pika.abc import AbstractRobustExchange, AbstractRobustQueue, AbstractIncomingMessage


# def exit_rabbitm_test(rabbitmq: RabbitmqAsync):
#     # Выходим из канала
#     rabbitmq.queue_delete()
#     exit(1)


