import datetime

from logsmal import logger, loglevel
from pika.exchange_type import ExchangeType

from base_rabbitmq import Rabbitmq

logger.info = loglevel(title_logger='[INFO][Отправитель]', fileout='./test.log')


#
# @Rabbitmq.connect(exchange="AnyName")
# @Queue
# def producer(**kwargs):
#     send(**kwargs)
#
#
# @Rabbitmq.connect(exchange="any_name")
# @Rabbitmq.Exchange()
# def producer_echange_fanout(**kwargs):
#     send(**kwargs)
#
#

def send_producer(rabbitmq: Rabbitmq):
    # Отправить сообщение в очередь
    rabbitmq.chanel.basic_publish(
        exchange=rabbitmq.exchange[0],
        routing_key=rabbitmq.routing_key,
        # Данные сообщения
        body=f"привет мир как дела ?{datetime.datetime.now()}".encode("utf-8")
    )


@Rabbitmq.connect()
@Rabbitmq.Exchange(exchange="te", exchange_type=ExchangeType.fanout)
def producer_echange_fanout(rabbitmq: Rabbitmq):
    rabbitmq.routing_key = ""
    send_producer(rabbitmq)


if __name__ == '__main__':
    producer_echange_fanout()
