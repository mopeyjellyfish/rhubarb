import asyncio
import json
from contextlib import suppress
from logging import Logger

from hypothesis import given
from hypothesis import strategies as st
from pytest import fixture, mark, raises

from rhubarb._core import Rhubarb, UnknownBackend, Unsubscribed
from rhubarb.backends.base import BaseBackend


@fixture
async def queue(URL):
    async with Rhubarb(URL) as events:
        yield events


@fixture
async def subscriber(queue):
    async with queue.subscribe("test-channel") as subscriber:
        yield subscriber


@fixture
async def queue_json(URL):
    async with Rhubarb(URL, serializer=json.dumps, deserializer=json.loads) as events:
        yield events


@fixture
async def subscriber_json(queue_json):
    async with queue_json.subscribe("test-channel") as subscriber:
        yield subscriber


@mark.asyncio
class TestRhubarb:
    async def test_queue_interface(self, URL):
        async with Rhubarb(URL) as queue:
            assert hasattr(queue, "connect")
            assert hasattr(queue, "disconnect")
            assert hasattr(queue, "publish")
            assert isinstance(queue._backend, BaseBackend)
            assert queue.logger
            assert queue._lock
            assert queue._subscribers == {}

    async def test_unknown_backend(self):
        with raises(UnknownBackend):
            async with Rhubarb("xyz://") as _:
                pass

    @given(
        message=st.text(
            alphabet=st.characters(blacklist_categories=("Cs", "Cc")),
        )
    )
    async def test_subscribe_and_publish(self, queue, subscriber, message):
        await queue.publish("test-channel", message)
        event = await subscriber.get()
        assert event.channel == "test-channel"
        assert event.message == message

    async def test_multiple_subscribers(self, queue):
        async with queue.subscribe("test-channel") as first_subscriber:
            async with queue.subscribe("test-channel") as second_subscriber:
                assert first_subscriber is not second_subscriber
                assert first_subscriber != second_subscriber
                assert "test-channel" in queue._subscribers
                assert len(queue._subscribers["test-channel"]) == 2

        assert "test-channel" not in queue._subscribers

    async def test_subscribe_multiple_channels(self, queue):
        async with queue.subscribe("test-channel-1") as first_subscriber:
            async with queue.subscribe("test-channel-2") as second_subscriber:
                assert first_subscriber is not second_subscriber
                assert first_subscriber != second_subscriber
                assert "test-channel-1" in queue._subscribers
                assert "test-channel-2" in queue._subscribers
                assert len(queue._subscribers["test-channel-1"]) == 1
                assert len(queue._subscribers["test-channel-2"]) == 1

        assert "test-channel-1" not in queue._subscribers
        assert "test-channel-2" not in queue._subscribers

    async def test_subscribers_multiple_channel_publish(self, queue):
        async with queue.subscribe("test-channel-1") as first_subscriber:
            async with queue.subscribe("test-channel-2") as second_subscriber:
                await asyncio.sleep(0)
                assert first_subscriber is not second_subscriber
                assert first_subscriber != second_subscriber
                assert "test-channel-1" in queue._subscribers
                assert "test-channel-2" in queue._subscribers
                assert len(queue._subscribers["test-channel-1"]) == 1
                assert len(queue._subscribers["test-channel-2"]) == 1
                await queue.publish("test-channel-1", "test-data-channel-1")
                await queue.publish("test-channel-2", "test-data-channel-2")
                event = await first_subscriber.get()
                assert event.message == "test-data-channel-1"
                event = await second_subscriber.get()
                assert event.message == "test-data-channel-2"

        assert "test-channel-1" not in queue._subscribers
        assert "test-channel-2" not in queue._subscribers

    async def test_iterate_subscriber_close_queue(self, queue):
        async with queue.subscribe("test-channel-1") as subscriber:
            await asyncio.sleep(0)
            await queue.publish("test-channel-1", "test 1")
            await queue.publish("test-channel-1", "test 2")
            assert await subscriber.get()
            assert await subscriber.get()
            await subscriber._queue.put(None)
            with raises(Unsubscribed):
                await subscriber.get()

    async def test_subscribe_iteration(self, queue):
        message_count = 100

        async def publish_channel():
            for n in range(message_count):
                await queue.publish("test-channel-1", f"message {n}")

        async with queue.subscribe("test-channel-1") as subscriber:
            asyncio.create_task(publish_channel())
            count = 0
            async for event in subscriber:
                assert event.channel == "test-channel-1"
                assert event.message == f"message {count}"
                count += 1
                if message_count == count:
                    break

            assert subscriber._queue.qsize() == 0

    async def test_queue_ends_with_none_event(self, queue):
        async def add_none_event():
            await asyncio.sleep(0)
            for sub_queue in queue._subscribers["test-channel"]:
                await sub_queue.put(None)

        async with queue.subscribe("test-channel") as subscriber:
            asyncio.create_task(add_none_event())
            async for event in subscriber:
                assert event

    async def test_connect_disconnect(self, URL):
        queue = Rhubarb(URL)
        await queue.connect()
        assert not queue._reader_task.done()
        assert isinstance(queue._reader_task, asyncio.Task)
        assert not queue._reader_task.done()
        await queue.disconnect()
        assert queue._reader_task.done()

    async def test_disconnect_reader_stopped(self, URL):
        queue = Rhubarb(URL)
        await queue.connect()
        assert not queue._reader_task.done()
        queue._reader_task.cancel()
        with suppress(asyncio.exceptions.CancelledError):
            await queue._reader_task
        await queue.disconnect()
        assert queue._reader_task.done()

    async def test_publish_subscribe_json(self, queue_json, subscriber_json):
        test_message = {"test_key": "test_value"}
        await queue_json.publish("test-channel", test_message)
        event = await subscriber_json.get()
        assert event.channel == "test-channel"
        assert event.message == test_message
