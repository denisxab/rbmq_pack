from datetime import datetime
from re import match
from time import sleep
from uuid import uuid4
from functools import partial

import pika
import pytest
from logsmal import logger, loglevel
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exchange_type import ExchangeType

from rabbitmq_pack.base_rabbitmq import Rabbitmq, RABBITMQ_PARAMS
from rabbitmq_pack.logic_massage import getMessageSuccess
from rabbitmq_pack.test.test_rabbitmq.test_helpful import exit_rabbitm_test, send_consumer_T, run_threads, \
    send_producer_T

logger.rabbitmq_info = loglevel(title_logger='[INFO]', fileout='./test.log')
logger.rabbitmq_info(f"{'-' * 10}{datetime.now()}{'-' * 10}", flag=f"START")
exchange_name = 'testw'


class Test_Exchange:

    def teardown(self):
        """
        Удалить распределители после успешного выполнения каждого тестового метода
        """
        channel_number = 1
        # Подключиться к ``Rabbitmq``
        rabbitmq_cli = BlockingConnection(RABBITMQ_PARAMS)
        # Подключиться к каналу
        with rabbitmq_cli.channel(channel_number=channel_number) as rabbitmq_chanel:
            rabbitmq: Rabbitmq = Rabbitmq(rabbitmq_chanel=rabbitmq_chanel)
            rabbitmq.chanel.exchange_delete(exchange_name)

    @staticmethod
    @pytest.mark.parametrize(("name_queue",),
                             [
                                 (exchange_name,)
                             ])
    def test_client_server(name_queue):
        """
        Проверка клиент серверного варианта работы.
        Между клиентом и сервером будет очередь на отправку и получения.

        https://i.imgur.com/EHKopYx.png
        """

        data_server = ""
        data_client = ""

        @Rabbitmq.Connect()
        # Отправляем клиенту сообщение
        @Rabbitmq.Queue(name=name_queue)
        def server(rabbitmq: Rabbitmq):
            def self_(ch: BlockingChannel, mh, props, body):
                nonlocal data_server
                # Вывожу сообщение клиента
                data_server = f"Получил сообщение: {body.decode()}\nот id: {props.correlation_id}\nОчередь: {props.reply_to}"
                # Отправляют подтверждение получения ответа клиенту
                ch.basic_publish(
                    exchange='',
                    # Возвращаю в путь по которому пришло сообщение
                    routing_key=props.reply_to,
                    body=f"Сообщение от сервера: Приято сообщение по id {props.correlation_id}".encode()
                )
                # Выходим из канала
                exit_rabbitm_test(rabbitmq)

            # Указываем логику работы с полученным сообщением от сервера
            rabbitmq.chanel.basic_consume(
                queue=rabbitmq.queue[0],
                on_message_callback=partial(
                    getMessageSuccess,
                    callback=self_
                )
            )

            # Ожидаем сообщений
            rabbitmq.chanel.start_consuming()

        @Rabbitmq.Connect()
        # Случайная очередь для отправки серверу информации
        @Rabbitmq.Queue(exclusive=True)
        # Очередь дял получения данных с сервера
        @Rabbitmq.Queue(name=name_queue)
        def client(rabbitmq: Rabbitmq):
            def self_(body, **kwargs):
                nonlocal data_client
                data_client = f"reply recieved: {body.decode()}"
                # Выходим из канала
                exit_rabbitm_test(rabbitmq)

            # Указываем логику работы с полученным сообщением от сервера
            rabbitmq.chanel.basic_consume(
                # Случайная уникальная очередь
                queue=rabbitmq.queue[0],
                # Что делать с полученным сообщением
                on_message_callback=partial(
                    getMessageSuccess,
                    callback=self_
                )
            )
            # Готовая функция для ``basic_publish`` в которую нужно отправить только ``body``
            call_publish = rabbitmq.call_publish(
                # Указать обратную связь
                properties=lambda: pika.BasicProperties(
                    # В какую очередь вернуть ответ
                    reply_to=rabbitmq.queue[0],
                    # ID сообщения
                    correlation_id=str(uuid4()),
                ),
                routing_key=rabbitmq.queue[1],
                exchange=''
            )

            #: Отправить сообщение на сервер
            call_publish(f"Сообщение от клиента 1 - {datetime.now()}".encode("utf-8"))
            call_publish(f"Сообщение от клиента 2 - {datetime.now()}".encode("utf-8"))

            # Ожидаем сообщений
            rabbitmq.chanel.start_consuming()

        run_threads([client, server])

        assert match(
            'Получил сообщение: Сообщение от клиента 1 - \d+-\d+\d',
            data_server) is not None
        assert match(
            'reply recieved: Сообщение от сервера: Приято сообщение по id [\w\d]+-[\w\d]+-[\w\d]+-[\w\d]+-[\w\d]+',
            data_client) is not None

    @staticmethod
    @pytest.mark.parametrize(("exchange",),
                             [
                                 (exchange_name,)
                             ])
    def test_topic(exchange):
        """
        Проверка работы отправки одно и того же сообщения в разные очереди
        на основе тем.

        :param exchange:
        :return:
        """
        data_consumer1 = [""]
        data_consumer2 = [""]

        @Rabbitmq.Connect()
        # Подключаемся к обменнику
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.topic)
        # Получаем сообщения в свою очередь, которые имеют указанны путь (в данном случае совпадает по шаблону)
        # Это сообщения не удалиться из очереди, оно будет также доступно другим очередям.
        @Rabbitmq.Queue(routing_key=("*.git",), exclusive=True)
        def consumer_echange_topic(rabbitmq):
            send_consumer_T(rabbitmq, data_consumer1)

        # То же самое только отслеживается другой путь
        @Rabbitmq.Connect()
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.topic)
        @Rabbitmq.Queue(routing_key=("user.#",), exclusive=True)
        def consumer_echange_topic2(rabbitmq):
            send_consumer_T(rabbitmq, data_consumer2)

        @Rabbitmq.Connect()
        # Подключаемся к обменнику
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.topic)
        def producer_echange_topic(rabbitmq):
            # !! Специальная задержка для того чтобы успели создастся очереди у слушателей
            sleep(1)
            # Отправляем сообщение с указанной темой,
            # слушатели которые обрабатывают данный путь (или шаблон)
            # получат это сообщение
            rabbitmq.routing_key = "user.git"
            send_producer_T(rabbitmq)

        run_threads([
            consumer_echange_topic,
            consumer_echange_topic2,
            producer_echange_topic,
        ])

        assert match(
            'Получил сообщение: привет мир как дела ?[\w\W]+]',
            data_consumer1[0]) is not None
        assert match(
            'Получил сообщение: привет мир как дела ?[\w\W]+]',
            data_consumer2[0]) is not None

    @staticmethod
    @pytest.mark.parametrize(("exchange",),
                             [
                                 (exchange_name,)
                             ])
    def test_direct(exchange):
        """
        Проверка отправки одно и того же сообщения слушателям,
        на основание отслеживания точного пути.
        """
        data_consumer1 = [""]
        data_consumer2 = [""]

        @Rabbitmq.Connect()
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.direct)
        # Указываем какие точные пути мы будем отслеживать для данной очереди
        @Rabbitmq.Queue(routing_key=("pip",), exclusive=True)
        def consumer_echange_direct(rabbitmq: Rabbitmq):
            send_consumer_T(rabbitmq, data_consumer1)

        @Rabbitmq.Connect()
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.direct)
        # Указываем какие точные пути мы будем отслеживать для данной очереди
        @Rabbitmq.Queue(routing_key=("git", 'pip'), exclusive=True)
        def consumer_echange_direct2(rabbitmq: Rabbitmq):
            send_consumer_T(rabbitmq, data_consumer2)

        @Rabbitmq.Connect()
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.direct)
        def producer_echange_direct(rabbitmq: Rabbitmq):
            # !! Специальная задержка для того чтобы успели создастся очереди у слушателей
            sleep(1)
            rabbitmq.routing_key = "pip"
            send_producer_T(rabbitmq)

        run_threads([
            consumer_echange_direct,
            consumer_echange_direct2,
            producer_echange_direct,
        ])

        assert match(
            'Получил сообщение: привет мир как дела ?[\w\W]+]',
            data_consumer1[0]) is not None
        assert match(
            'Получил сообщение: привет мир как дела ?[\w\W]+]',
            data_consumer2[0]) is not None

    @staticmethod
    @pytest.mark.parametrize(("exchange",),
                             [
                                 (exchange_name,)
                             ])
    def test_fount(exchange):
        """
        Проверка отправки одного и того же сообщения в разные очереди
        маршрутизации на основе имени ``exchange``

        """
        data_consumer1 = [""]
        data_consumer2 = [""]

        @Rabbitmq.Connect()
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.fanout)
        @Rabbitmq.Queue(routing_key=('',), exclusive=True)
        def consumer_echange_fanout(rabbitmq: Rabbitmq):
            send_consumer_T(rabbitmq, data_consumer1)

        @Rabbitmq.Connect()
        @Rabbitmq.Exchange(name=exchange, type_=ExchangeType.fanout)
        @Rabbitmq.Queue(routing_key=('',), exclusive=True)
        def consumer_echange_fanout2(rabbitmq: Rabbitmq):
            send_consumer_T(rabbitmq, data_consumer2)

        @Rabbitmq.Connect()
        @Rabbitmq.Exchange(name=exchange)
        def producer_echange_fanout(rabbitmq: Rabbitmq):
            # !! Специальная задержка для того чтобы успели создастся очереди у слушателей
            sleep(1)
            rabbitmq.routing_key = ''
            send_producer_T(rabbitmq)

        run_threads([
            consumer_echange_fanout,
            consumer_echange_fanout2,
            producer_echange_fanout,
        ])

        assert match(
            'Получил сообщение: привет мир как дела ?[\w\W]+]',
            data_consumer1[0]) is not None
        assert match(
            'Получил сообщение: привет мир как дела ?[\w\W]+]',
            data_consumer2[0]) is not None
