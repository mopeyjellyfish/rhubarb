from collections.abc import AsyncGenerator

import pytest_asyncio
from pytest import mark

from rhubarb.backends.memory import MemoryBackend
from rhubarb.event import Event


@pytest_asyncio.fixture
async def memory(MEMORY_URL: str) -> AsyncGenerator[Event, None]:
    memory_backend = MemoryBackend(MEMORY_URL)
    await memory_backend.connect()
    yield memory_backend
    await memory_backend.disconnect()


def test_memory_queue(memory):
    assert hasattr(memory, "connect")
    assert hasattr(memory, "disconnect")
    assert hasattr(memory, "subscribe")
    assert hasattr(memory, "unsubscribe")
    assert hasattr(memory, "publish")
    assert hasattr(memory, "next_event")
    assert memory._channels == set()


@mark.asyncio
class TestMemoryBackend:
    async def test_memory_connect_disconnect(self, MEMORY_URL):
        memory = MemoryBackend(MEMORY_URL)
        await memory.connect()
        assert memory._consumer
        await memory.disconnect()

    async def test_memory_subscribe(self, memory):
        await memory.subscribe("test-channel")
        assert "test-channel" in memory._channels

    async def test_memory_unsubscribe(self, memory):
        await memory.subscribe("test-channel")
        await memory.unsubscribe("test-channel")
        assert "test-channel" not in memory._channels

    async def test_memory_unsubscribe_unknown_channel(self, memory):
        await memory.unsubscribe("unknown-channel-name")

    async def test_duplicate_subscribe_request(self, memory):
        await memory.subscribe("test-channel")
        original_pub_sub = memory._channels
        await memory.subscribe("test-channel")
        new_pub_sub = memory._channels
        assert original_pub_sub is new_pub_sub

    async def test_memory_publish(self, memory):
        await memory.subscribe("test-channel")
        await memory.subscribe("test-channel-a")
        await memory.publish("test-channel", "test data")
        event = await memory.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data"
        await memory.publish("test-channel", "test data A")
        event = await memory.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data A"
        await memory.publish("test-channel-a", "test data B")
        event = await memory.next_event()
        assert event.channel == "test-channel-a"
        assert event.message == "test data B"
