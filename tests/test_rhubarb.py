import asyncio
import json
from asyncio.exceptions import TimeoutError
from contextlib import suppress
from multiprocessing import Process

import pytest
import pytest_asyncio
from anyio import Event
from async_timeout import timeout
from hypothesis import given
from hypothesis import strategies as st
from pytest import mark, raises

from rhubarb._core import Rhubarb, SubscribeError, UnknownBackend, Unsubscribed
from rhubarb.backends.base import BaseBackend
from rhubarb.backends.exceptions import HistoryError


@pytest_asyncio.fixture
async def queue(URL):
    async with Rhubarb(URL) as events:
        yield events


@pytest_asyncio.fixture
async def subscriber(queue):
    async with queue.subscribe("test-channel") as subscriber:
        yield subscriber


@pytest_asyncio.fixture
async def queue_json(URL):
    async with Rhubarb(URL, serializer=json.dumps, deserializer=json.loads) as events:
        yield events


@pytest_asyncio.fixture
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
        await queue.connect()
        assert not queue._reader_task.done()
        queue._reader_task.cancel()
        with suppress(asyncio.exceptions.CancelledError):
            await queue._reader_task
        await queue.disconnect()
        assert queue._reader_task.done()

    async def test_duplicate_connect_warning(self, URL, caplog):
        queue = Rhubarb(URL)
        await queue.connect()
        await queue.connect()
        assert (
            "Already connected 'True', was 'connect' called more than once?"
            in caplog.text
        )
        await queue.disconnect()

    async def test_publish_subscribe_json(self, queue_json, subscriber_json):
        test_message = {"test_key": "test_value"}
        await queue_json.publish("test-channel", test_message)
        event = await subscriber_json.get()
        assert event.channel == "test-channel"
        assert event.message == test_message

    async def test_subscribe_history(self, URL, queue):
        supported = ["redis://localhost:6379/0", "kafka://localhost:9092"]
        if URL in supported:
            events = list(range(10))
            for i in events:
                await queue.publish("test-channel", f"{i}")

            async with queue.subscribe("test-channel", history=10) as subscriber:
                read_events = []
                count = 0
                async for event in subscriber:
                    read_events.append(event.message)
                    count += 1
                    if count == len(events):  # read the last events
                        break

                assert all(
                    (
                        str(event) == read_event
                        for event, read_event in zip(events, read_events)
                    )
                )
        else:
            with raises(HistoryError, match="History not supported for backend"):
                async with queue.subscribe("test-channel", history=10):
                    return

    async def test_disconnect_subscribers_are_closed(self, URL):
        """test that an event object disconnecting also stops subscribers gracefully"""
        queue = Rhubarb(URL)
        await queue.connect()
        async with queue.subscribe("test-channel") as subscriber:
            await queue.disconnect()  # while subscribed to a channel in one co-routine its possible to disconnect the event bus in another co routine.
            async for _ in subscriber:  # should not iterate anything here, if the subscriber queues were not closed then this would block.
                pass

        assert "test-channel" not in queue._subscribers

    async def test_cancel_coroutine_subscriber(self, queue):
        async def subscriber(subscribed_event):
            async with queue.subscribe("test-channel") as subscriber:
                subscribed_event.set()
                async for _ in subscriber:  # Iterate the subscriber to produce a `asyncio.exceptions.CancelledError` when the task is cancelled
                    pass

        subscribed_event = Event()
        task = asyncio.create_task(subscriber(subscribed_event))
        await subscribed_event.wait()
        task.cancel()
        with suppress(
            asyncio.exceptions.CancelledError
        ):  # not an exception we need to assert for
            await task
        assert (
            "test-channel" not in queue._subscribers
        )  # if the subscriber was unsubscribed correctly then they won't appear in the subscribers.

    async def test_connecting_in_child_process(self, queue, URL):
        if "memory" not in URL:  # skip memory backend
            events = Rhubarb(URL)  # make the event bus in one event loop

            def event_connect(event_bus):
                async def event_bus_run(child_event_bus):
                    await child_event_bus.connect()  # connect in another event loop
                    async with child_event_bus.subscribe("test-channel") as subscriber:
                        async for _ in subscriber:  # Iterate the subscriber to produce a `asyncio.exceptions.CancelledError` when the task is cancelled
                            break
                    await child_event_bus.disconnect()

                loop = asyncio.new_event_loop()

                loop.run_until_complete(event_bus_run(event_bus))
                loop.close()

            p = Process(target=event_connect, args=(events,))
            p.start()
            await asyncio.sleep(0.5)  # let the process start up
            await queue.publish("test-channel", "test")
            p.join()

    async def test_group_subscribe(self, URL):
        expected_messages = list(str(i) for i in range(10))

        async def read_subscriptions(subscriber):
            tasks = []
            with suppress(asyncio.exceptions.TimeoutError):
                async with timeout(1):
                    async for event in subscriber:
                        tasks.append(event)
                        await asyncio.sleep(0)
            return tasks

        if "redis" in URL:  # currently only supported by redis
            async with Rhubarb(URL) as queue_1, Rhubarb(URL) as queue_2:
                async with queue_1.subscribe(
                    "TEST-GROUP-CHANNEL", group_name="TEST_GROUP", consumer_name="sub_1"
                ) as subscriber_1, queue_2.subscribe(
                    "TEST-GROUP-CHANNEL", group_name="TEST_GROUP", consumer_name="sub_2"
                ) as subscriber_2:
                    await asyncio.sleep(0)
                    for message in expected_messages:
                        await queue_1.publish("TEST-GROUP-CHANNEL", message)
                    events = await asyncio.gather(
                        read_subscriptions(subscriber_1),
                        read_subscriptions(subscriber_2),
                    )
                    read_messages = [event.message for event in events[0]] + [
                        event.message for event in events[1]
                    ]
                    read_messages.sort()
                    assert read_messages == expected_messages
        else:
            pytest.xfail(f"{URL} backend not supported")

    async def test_group_subscribe_errors_no_consumer(self, queue_json):
        """If specifying a group but not a name or a name but not a group that an error occurs"""
        with raises(SubscribeError):
            async with queue_json.subscribe("TEST-GROUP-CHANNEL", group_name="group"):
                pass

    async def test_group_subscribe_errors_no_group(self, queue_json):
        """If specifying a group but not a name or a name but not a group that an error occurs"""
        with raises(SubscribeError):
            async with queue_json.subscribe("TEST-GROUP-CHANNEL", consumer_name="name"):
                pass

    async def test_group_subscribe_errors(self, URL):
        """test that other backends have an error during subscribing"""
        if "redis" not in URL:  # currently only supported by redis
            with pytest.raises(NotImplementedError):
                async with Rhubarb(URL) as queue:
                    async with queue.subscribe(
                        "TEST-GROUP-CHANNEL",
                        group_name="TEST_GROUP",
                        consumer_name="sub_1",
                    ) as subscriber:
                        async for _ in subscriber:
                            # nothing to read
                            break

        else:
            pytest.xfail(f"{URL} backend supports group consume")
