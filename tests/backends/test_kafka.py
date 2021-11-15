from typing import AsyncGenerator

import asyncio

from pytest import fixture, mark, raises

from rhubarb.backends.exceptions import UnsubscribeError
from rhubarb.backends.kafka import KafkaBackend
from rhubarb.event import Event


@fixture
async def kafka(KAFKA_URL: str) -> AsyncGenerator[Event, None]:
    kafka_backend = KafkaBackend(url=KAFKA_URL)
    await kafka_backend.connect()
    yield kafka_backend
    await kafka_backend.disconnect()


@mark.asyncio
class TestKafkaBackend:
    def test_kafka_queue(self, kafka, KAFKA_URL):
        assert hasattr(kafka, "connect")
        assert hasattr(kafka, "disconnect")
        assert hasattr(kafka, "subscribe")
        assert hasattr(kafka, "unsubscribe")
        assert hasattr(kafka, "publish")
        assert hasattr(kafka, "next_event")

    async def test_kafka_connect_disconnect(self, KAFKA_URL):
        kafka = KafkaBackend(url=KAFKA_URL)
        await kafka.connect()
        assert kafka._servers
        assert kafka._producer
        await kafka.disconnect()

    async def test_kafka_subscribe(self, kafka):
        await kafka.subscribe("test-channel")
        assert kafka._consumer
        assert kafka._consumer_reader_task
        assert "test-channel" in kafka._channels

    async def test_kafka_unsubscribe(self, kafka):
        await kafka.subscribe("test-channel")
        await kafka.unsubscribe("test-channel")
        assert "test-channel" not in kafka._channels

    async def test_unsubscribe_unknown_channel(self, kafka):
        with raises(UnsubscribeError, match="Unknown channel unknown-channel-name"):
            await kafka.unsubscribe("unknown-channel-name")

    async def test_kafka_unsubscribe_one_of_many(self, kafka):
        await kafka.subscribe("test-channel")
        await kafka.subscribe("test-channel-a")
        await kafka.unsubscribe("test-channel")
        assert "test-channel" not in kafka._channels
        assert "test-channel-a" in kafka._channels

    async def test_duplicate_subscribe_request(self, kafka):
        await kafka.subscribe("test-channel")
        original_channels = kafka._channels
        await kafka.subscribe("test-channel")
        assert original_channels is kafka._channels

    async def test_kafka_publish(self, kafka):
        await kafka.subscribe("test-channel")
        await kafka.subscribe("test-channel-a")
        await kafka.publish("test-channel", "test data")
        event = await kafka.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data"
        await kafka.publish("test-channel", "test data A")
        event = await kafka.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data A"
        await kafka.publish("test-channel-a", "test data B")
        event = await kafka.next_event()
        assert event.channel == "test-channel-a"
        assert event.message == "test data B"