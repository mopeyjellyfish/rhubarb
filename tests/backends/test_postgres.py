from typing import AsyncGenerator

import asyncio

from pytest import fixture, mark, raises

from rhubarb.backends.postgres import AsyncPgBackend
from rhubarb.event import Event


@fixture
async def asyncpg(POSTGRES_URL: str) -> AsyncGenerator[Event, None]:
    asyncpg_backend = AsyncPgBackend(url=POSTGRES_URL)
    await asyncpg_backend.connect()
    yield asyncpg_backend
    await asyncpg_backend.disconnect()


@mark.asyncio
class TestAsyncPgBackend:
    def test_asyncpg_queue(self, asyncpg, POSTGRES_URL):
        assert hasattr(asyncpg, "connect")
        assert hasattr(asyncpg, "disconnect")
        assert hasattr(asyncpg, "subscribe")
        assert hasattr(asyncpg, "unsubscribe")
        assert hasattr(asyncpg, "publish")
        assert hasattr(asyncpg, "next_event")
        assert asyncpg.url == POSTGRES_URL

    async def test_asyncpg_connect_disconnect(self, POSTGRES_URL):
        asyncpg = AsyncPgBackend(url=POSTGRES_URL)
        await asyncpg.connect()
        assert asyncpg._connection
        assert asyncpg._listen_queue
        await asyncpg.disconnect()

    async def test_asyncpg_subscribe(self, asyncpg):
        await asyncpg.subscribe("test-channel")

    async def test_asyncpg_unsubscribe(self, asyncpg):
        await asyncpg.subscribe("test-channel")
        await asyncpg.unsubscribe("test-channel")
        assert "test-channel" not in asyncpg._channels

    async def test_asyncpg_unsubscribe_unknown_channel(self, asyncpg):
        await asyncpg.unsubscribe("unknown-channel-name")

    async def test_duplicate_subscribe_request(self, asyncpg):
        await asyncpg.subscribe("test-channel")
        original_channels = asyncpg._channels
        await asyncpg.subscribe("test-channel")
        new_channels = asyncpg._channels
        assert original_channels is new_channels

    async def test_asyncpg_publish(self, asyncpg):
        await asyncpg.subscribe("test-channel")
        await asyncpg.subscribe("test-channel-a")
        await asyncpg.publish("test-channel", "test data")
        event = await asyncpg.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data"
        await asyncpg.publish("test-channel", "test data A")
        event = await asyncpg.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data A"
        await asyncpg.publish("test-channel-a", "test data B")
        event = await asyncpg.next_event()
        assert event.channel == "test-channel-a"
        assert event.message == "test data B"
