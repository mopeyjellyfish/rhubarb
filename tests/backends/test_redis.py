from typing import AsyncGenerator

import asyncio

from aioredis.exceptions import ConnectionError
from pytest import fixture, mark, raises

from rhubarb.backends.exceptions import UnsubscribeError
from rhubarb.backends.redis import RedisBackend
from rhubarb.event import Event


@fixture
async def redis(REDIS_URL: str) -> AsyncGenerator[Event, None]:
    redis_backend = RedisBackend(url=REDIS_URL)
    await redis_backend.connect()
    yield redis_backend
    await redis_backend.disconnect()


@mark.asyncio
class TestRedisBackend:
    def test_redis_queue(self, redis, REDIS_URL):
        assert hasattr(redis, "connect")
        assert hasattr(redis, "disconnect")
        assert hasattr(redis, "subscribe")
        assert hasattr(redis, "unsubscribe")
        assert hasattr(redis, "publish")
        assert hasattr(redis, "next_event")
        assert redis.url == REDIS_URL

    async def test_redis_connect_disconnect(self, REDIS_URL):
        redis = RedisBackend(url=REDIS_URL)
        await redis.connect()
        assert redis._redis
        await redis.disconnect()

    async def test_redis_connect_invalid(self):
        with raises(
            ConnectionError, match=r"Error -\d+ connecting to invalid_url:6379. -\d+."
        ):  # specific error code appears to differ between systems.
            redis = RedisBackend(url="redis://invalid_url")
            await redis.connect()

    async def test_redis_subscribe(self, redis):
        await redis.subscribe("test-channel")
        await asyncio.sleep(0)

    async def test_redis_unsubscribe(self, redis):
        await redis.subscribe("test-channel")
        await redis.unsubscribe("test-channel")
        assert "test-channel" not in redis._channels

    async def test_redis_unsubscribe_unknown_channel(self, redis):
        with raises(UnsubscribeError, match="Unknown channel unknown-channel-name"):
            await redis.unsubscribe("unknown-channel-name")

    async def test_duplicate_subscribe_request(self, redis):
        await redis.subscribe("test-channel")
        original_pub_sub = redis._channels["test-channel"]
        await redis.subscribe("test-channel")
        new_pub_sub = redis._channels["test-channel"]
        assert original_pub_sub is new_pub_sub

    async def test_redis_publish(self, redis):
        await redis.subscribe("test-channel")
        await redis.subscribe("test-channel-a")
        await redis.subscribe("test-channel-b")
        await asyncio.sleep(0)
        await redis.publish("test-channel", "test data")
        event = await redis.next_event()
        assert event.channel == "test-channel"
        assert event.message == "test data"
        await redis.publish("test-channel", "Another messsage")
        event = await redis.next_event()
        assert event.channel == "test-channel"
        assert event.message == "Another messsage"
        await redis.publish("test-channel-a", "Just a message")
        event = await redis.next_event()
        assert event.channel == "test-channel-a"
        assert event.message == "Just a message"

    async def test_redis_history_subscribe(self, redis):
        await redis.publish("test-channel-1", "test-message")
        historic_events = await redis.history("test-channel-1", 1)
        await redis.subscribe("test-channel-1")
        await asyncio.sleep(0)
        assert len(historic_events) == 1
        assert historic_events[0].message == "test-message"
        assert redis._channel_latest_id["test-channel-1"]
