import asyncio
import json
from contextlib import suppress

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


@mark.asyncio
class TestRhubarb:
    async def test_queue_interface(self, URL):
        async with Rhubarb(URL) as queue:
            assert hasattr(queue, "connect")
            assert hasattr(queue, "disconnect")
            assert hasattr(queue, "publish")
            assert isinstance(queue._backend, BaseBackend)

    async def test_unknown_backend(self):
        with raises(UnknownBackend):
            async with Rhubarb("xyz://") as _:
                pass

    async def test_subscribe_and_publish(self, queue, subscriber):
        await queue.publish("test-channel", "test-data")
        event = await subscriber.get()
        assert event.channel == "test-channel"
        assert event.message == "test-data"

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

    async def test_subscribers_multiple_channels(self, queue):
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

    async def test_iterate_subscriber_close_queue(self, queue):
        async with queue.subscribe("test-channel-1") as subscriber:
            await queue.publish("test-channel-1", "test 1")
            await queue.publish("test-channel-1", "test 2")
            assert await subscriber.get()
            assert await subscriber.get()
            await subscriber._queue.put(None)
            with raises(Unsubscribed):
                await subscriber.get()

    async def test_subscribe_iteration(self, queue):
        message_count = 10

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
            await asyncio.sleep(0.1)
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

    async def test_publish_json(self, queue, subscriber):
        test_message = {"test_key": "test_value"}
        await queue.publish_json("test-channel", test_message)
        event = await subscriber.get()
        assert event.channel == "test-channel"
        assert event.message == json.dumps(test_message)

    async def test_subscribe_json(self, queue):
        test_message = {"test_key": "test_value"}
        async with queue.subscribe(
            "test-channel", message_loader=json.loads
        ) as subscriber:
            await queue.publish_json("test-channel", test_message)
            event = await subscriber.get()
            assert event.channel == "test-channel"
            assert event.message == test_message
