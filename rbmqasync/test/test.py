import sys
from asyncio import run
from datetime import datetime
from os import environ
from pathlib import Path
from sys import argv

from aio_pika import ExchangeType
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
    @RabbitmqAsync.Queue(bind={exchange_name: ("*.git",)}, exclusive=True)
    async def consumer_echange_topic1(rabbitmq: RabbitmqAsync):
        logger.info("consumer_1", "START")
        await rabbitmq.consume('0', rabbitmq.get_message)

    @staticmethod
    @RabbitmqAsync.Connect(RABBITMQ_URL)
    @RabbitmqAsync.Exchange(name=exchange_name, type_=ExchangeType.TOPIC)
    @RabbitmqAsync.Queue(bind={exchange_name: ("user.#",)}, exclusive=True)
    async def consumer_echange_topic2(rabbitmq: RabbitmqAsync):
        logger.info("consumer_2", "START")
        await rabbitmq.consume('qwe', rabbitmq.get_message)

    @staticmethod
    @RabbitmqAsync.Connect(RABBITMQ_URL)
    @RabbitmqAsync.Exchange(name=exchange_name, type_=ExchangeType.TOPIC)
    async def producer_echange_topic(rabbitmq: RabbitmqAsync):
        logger.info("producer", "START")
        await rabbitmq.publish(exchange_name, routing_key=("user.git",),
                               message=f"Hello {datetime.now()}")

    # async def main_lop():
    #     task = [
    #         consumer_echange_topic(),
    #         consumer_echange_topic2(),
    #         producer_echange_topic(),
    #     ]
    #     await asyncio.gather(*task)
    # threading.Thread(target=asyncio.run, args=(consumer_echange_topic(),), daemon=True).start()
    # threading.Thread(target=asyncio.run, args=(producer_echange_topic(),), daemon=True).start()
    # threading.Thread(target=asyncio.run, args=(consumer_echange_topic2,)).start()
    # asyncio.run(consumer_echange_topic2())
    # res = asyncio.run(main_lop())
    # assert match(
    #     'Получил сообщение: привет мир как дела ?[\w\W]+]',
    #     data_consumer1[0]) is not None
    # assert match(
    #     'Получил сообщение: привет мир как дела ?[\w\W]+]',
    #     data_consumer2[0]) is not None


if __name__ == '__main__':
    argv_ = argv[1]
    if argv_ == '1':
        run(te_topic.consumer_echange_topic1())
    elif argv_ == '2':
        run(te_topic.consumer_echange_topic2())
    elif argv_ == '3':
        run(te_topic.producer_echange_topic())
