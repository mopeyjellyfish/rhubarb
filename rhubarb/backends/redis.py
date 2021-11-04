from typing import Any, Dict, List, Union

import asyncio
import logging
from asyncio import exceptions
from collections import namedtuple

import aioredis
import async_timeout

from rhubarb.backends.base import BaseBackend
from rhubarb.backends.exceptions import UnsubscribeError
from rhubarb.event import Event

STOPWORD = "STOP"

Subscription = namedtuple("Subscription", ["pubsub", "task"])


class RedisBackend(BaseBackend):
    def __init__(self, url: str):
        """Backend for the message queue using ``aioredis`` to interface with redis

        :param url: The URL of the redis instance
        :type url: str
        """
        self.url: str = url
        self._channels: dict[str, Subscription] = {}
        self._listen_queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()
        self.logger = logging.getLogger("RedisBackend")

    async def connect(self) -> None:
        """Connection to the configured Redis URL using ``aioredis``.
        Execute a `ping` to check if the connection is valid.
        """
        self.logger.info("Connecting to %s", self.url)
        self._redis = aioredis.from_url(
            self.url, encoding="utf-8", decode_responses=True
        )
        await self._redis.ping()  # causes aioredis to connect immediately
        self.logger.info("Connected to %s", self.url)

    async def disconnect(self) -> None:
        """Gracefully close the queue and end the tasks, the redis connection gracefully closes itself during garbage collection."""
        self.logger.info("Disconnecting from %s", self.url)
        for channel, subscription in self._channels.items():
            await self.publish(channel, STOPWORD)
            await subscription.pubsub.unsubscribe(channel)
            subscription.task.cancel()
            await subscription.task
        self._listen_queue.put_nowait(None)
        self.logger.info("Disconnected from %s", self.url)

    async def _reader(self, channel: aioredis.client.PubSub) -> None:
        """Read data from the passed channel object and put events into a queue to be read by the caller.

        :param channel: The channel subscribed to
        :type channel: aioredis.client.PubSub
        """
        while True:
            try:
                async with async_timeout.timeout(1):
                    if (
                        message := await channel.get_message(
                            ignore_subscribe_messages=True, timeout=1.0
                        )
                    ) is not None:
                        self.logger.debug(
                            "Read message %s from channel: %s",
                            message["channel"],
                            message["data"],
                        )
                        if message["data"] == STOPWORD:
                            self.logger.info("Reader received stop message")
                            break
                        event = Event(
                            channel=message["channel"], message=message["data"]
                        )
                        self._listen_queue.put_nowait(event)
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                pass

    async def subscribe(self, channel: str) -> None:
        """Subscribe to the passed channel

        :param channel: channel name to subscribe to
        :type channel: str
        """
        self.logger.info("Subscribing to %s", channel)
        if channel in self._channels:
            self.logger.warning("Already subscribed to %s", channel)
        else:
            _pubsub = self._redis.pubsub()
            await _pubsub.subscribe(channel)
            task = asyncio.create_task(self._reader(_pubsub))
            self._channels[channel] = Subscription(_pubsub, task)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from the passed channel

        :param channel: channel name to subscribe to
        :type channel: str
        :raises UnsubscribeError: When unsubscribed from a channel
        """
        self.logger.info("Unsubscribing from %s", channel)
        if (subscription := self._channels.get(channel)) is not None:
            await self.publish(channel, STOPWORD)
            await subscription.pubsub.unsubscribe(channel)
            subscription.task.cancel()
            await subscription.task
            del self._channels[channel]
        else:
            self.logger.warning("Unknown channel %s", channel)
            raise UnsubscribeError(f"Unknown channel {channel}")

    async def publish(self, channel: str, message: Any) -> None:
        """Using the configured redis connection pool a publish a message to the provided channel

        :param channel: Channel name to use to publish the message to
        :type channel: str
        :param message: Data that is to be published to the channel
        :type message: Any
        """
        self.logger.debug("Publishing message %s to channel %s", message, channel)
        await self._redis.publish(channel, message)

    async def next_event(self) -> Union[Event, None]:
        """Return the next event from the queue that was read from all channels"""
        self.logger.debug("Getting next event from redis...")
        return await self._listen_queue.get()
