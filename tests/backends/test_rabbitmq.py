from typing import AsyncGenerator

import asyncio

from pytest import fixture, mark, raises

from rhubarb.backends.rabbitmq import RabbitMQBackend
from rhubarb.event import Event


@fixture
async def rabbit_mq(RABBIT_MQ_URL: str) -> AsyncGenerator[Event, None]:
    rabbit_mq_backend = RabbitMQBackend(url=RABBIT_MQ_URL)
    await rabbit_mq_backend.connect()
    yield rabbit_mq_backend
    await rabbit_mq_backend.disconnect()


@mark.asyncio
class TestRabbitMQBackend:
    def test_rabbit_mq_queue(self, rabbit_mq, RABBIT_MQ_URL):
        assert hasattr(rabbit_mq, "connect")
        assert hasattr(rabbit_mq, "disconnect")
        assert hasattr(rabbit_mq, "subscribe")
        assert hasattr(rabbit_mq, "unsubscribe")
        assert hasattr(rabbit_mq, "publish")
        assert hasattr(rabbit_mq, "next_event")
        assert rabbit_mq._url == RABBIT_MQ_URL

    async def test_rabbit_mq_connect_disconnect(self, RABBIT_MQ_URL):
        rabbit_mq = RabbitMQBackend(url=RABBIT_MQ_URL)
        await rabbit_mq.connect()
        assert rabbit_mq._connection
        await rabbit_mq.disconnect()

    async def test_rabbit_mq_subscribe(self, rabbit_mq):
        await rabbit_mq.subscribe("test-channel")
        assert "test-channel" in rabbit_mq._channels

    async def test_rabbit_mq_unsubscribe(self, rabbit_mq):
        await rabbit_mq.subscribe("test-channel")
        await rabbit_mq.unsubscribe("test-channel")
        assert "test-channel" not in rabbit_mq._channels

    async def test_rabbit_mq_unsubscribe_unknown_channel(self, rabbit_mq):
        await rabbit_mq.unsubscribe("unknown-channel-name")

    async def test_duplicate_subscribe_request(self, rabbit_mq):
        await rabbit_mq.subscribe("test-channel")
        original_pub_sub = rabbit_mq._channels["test-channel"]
        await rabbit_mq.subscribe("test-channel")
        new_pub_sub = rabbit_mq._channels["test-channel"]
        assert original_pub_sub is new_pub_sub

    async def test_rabbit_mq_publish(self, rabbit_mq):
        await rabbit_mq.subscribe("test-channel")
        await rabbit_mq.subscribe("test-channel-a")
        await rabbit_mq.publish("test-channel", "test data")
        event = await rabbit_mq.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data"
        await rabbit_mq.publish("test-channel", "test data A")
        event = await rabbit_mq.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data A"
        await rabbit_mq.publish("test-channel-a", "test data B")
        event = await rabbit_mq.next_event()
        assert event.channel == "test-channel-a"
        assert event.message == "test data B"
