from typing import Protocol

import aio_pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties


class T_on_message_callback(Protocol):
    """."""

    async def __call__(
            self,
            ch: BlockingChannel,
            mh: Basic.Deliver,
            props,
            body: bytes,

    ) -> None: ...


class T_on_message_callbackAsync(Protocol):
    """."""

    async def __call__(
            self,
            message: aio_pika.abc.AbstractIncomingMessage,
    ) -> None: ...


def getMessageSuccess(
        ch: BlockingChannel,
        mh: Basic.Deliver,
        props: BasicProperties,
        body: bytes,
        callback: T_on_message_callback = lambda: None
):
    """
    Успешное получение сообщения

    :param callback: Полезная функцию которую нужно выполнить
    :param ch: Канал откуда пришло сообщение
    :param mh: Данные про очередь
    :param props:
    :param body: Сырые данные от отправителя
    :return:

    .. code-block:: python

        rabbitmq.chanel.basic_consume(
            # Случайная уникальная очередь
            queue=rabbitmq.queue[0],
            # Что делать с полученным сообщением
            on_message_callback=partial(
                getMessageSuccess,
                callback=self_
            )
        )
    """
    # Вызвать пользовательскую функцию
    callback(body=body, ch=ch, mh=mh, props=props)
    #: Подтвердить получение сообщения.
    #: Убедитесь что отключено авто п отверждение
    #: rabbitmq_chanel.basic_consume(auto_ack=False)
    ch.basic_ack(delivery_tag=mh.delivery_tag)


async def getMessageSuccessAsync(
        message: aio_pika.abc.AbstractIncomingMessage,
        callback: T_on_message_callbackAsync = lambda: None
):
    """
    Успешное получение сообщения

    :param message:
    :param callback: Полезная функцию которую нужно выполнить
    """
    # Вызвать пользовательскую функцию
    await callback(message)
