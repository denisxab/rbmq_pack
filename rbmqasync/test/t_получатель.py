from functools import partial

from pika.exchange_type import ExchangeType

from base_rabbitmq import Rabbitmq, getMessageSuccess


# @Rabbitmq.connect(exchange="any_name")
# @Queue
# def consumer(**kwargs):
#     send(**kwargs)
#
#


def send_consumer(rabbitmq: Rabbitmq):
    def self_(body, **kwargs):
        print(f"Получил сообщение: {body.decode('utf-8')}\nВ очередь {rabbitmq.queue}")

    rabbitmq.chanel.basic_consume(
        rabbitmq.queue[0],
        #: True - Автоматически подтверждать при получении сообщения. Это не всегда хорошо использовать,
        #: например если у нас долгая задача, то лучше
        #: потвердеть сообщение в ручную, когда эта задачи выполнится, это нужно чтобы брокер ``RabbitMQ``
        #: не заваливал нас сообщениями, а передовая их другим получателям.
        # auto_ack=True,
        #: Вызвать функцию при получении сообщения.
        #: В данном случае мы добавляем аргумент в функцию через ``partial``
        on_message_callback=partial(
            getMessageSuccess,
            # Что-то делаем с сообщением
            callback=self_
        )
    )
    rabbitmq.chanel.start_consuming()


@Rabbitmq.connect()
@Rabbitmq.Exchange(exchange="te", exchange_type=ExchangeType.fanout)
@Rabbitmq.Queue(routing_key=(None,), random_exclusive_queue=True)
def consumer_echange_fanout(rabbitmq: Rabbitmq):
    send_consumer(rabbitmq)


@Rabbitmq.connect()
@Rabbitmq.Exchange(exchange="te2", exchange_type=ExchangeType.fanout)
@Rabbitmq.Queue(routing_key=(None,), random_exclusive_queue=True)
def consumer_echange_fanout2(rabbitmq: Rabbitmq):
    send_consumer(rabbitmq)


if __name__ == '__main__':
    # consumer_echange_direct()
    # consumer_echange_fanout()
    consumer_echange_fanout2()
