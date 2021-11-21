from typing import Any, AsyncIterator, List, Union

import asyncio
import logging
from contextlib import suppress
from logging import Logger

import aioredis

from rhubarb.backends.base import BaseBackend
from rhubarb.event import Event


class RedisBackend(BaseBackend):
    def __init__(self, url: str):
        """Backend for the message queue using ``aioredis`` to interface with redis

        :param url: The URL of the redis instance
        :type url: str
        """
        self.url: str = url
        self._channels: dict[str, asyncio.Task[None]] = {}  # the channels subscribed to
        self._channel_latest_id: dict[str, int] = {}
        self.logger: Logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        """Connection to the configured Redis URL using ``aioredis``.
        Execute a `ping` to check if the connection is valid.
        """
        self.logger.info("Connecting to '%s'", self.url)
        self._listen_queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()
        self._redis = aioredis.from_url(
            self.url, encoding="utf-8", decode_responses=True
        )
        await self._redis.ping()  # causes aioredis to connect immediately
        self.logger.info("Connected to '%s'", self.url)

    async def disconnect(self) -> None:
        """Gracefully close the queue and end the tasks, the redis connection gracefully closes itself during garbage collection."""
        self.logger.info("Disconnecting from %s", self.url)
        for channel in list(self._channels.keys()):
            await self.unsubscribe(channel)
        self.logger.info("Disconnected from %s", self.url)

    async def subscribe(self, channel: str) -> None:
        """Subscribe to the passed channel

        :param channel: channel name to subscribe to
        :type channel: str
        """
        self.logger.info("Subscribing to '%s'", channel)
        if channel in self._channels:
            self.logger.warning("Already subscribed to '%s'", channel)
        else:
            self._channels[channel] = asyncio.create_task(self._reader(channel))
        self.logger.info("Subscribed to '%s'", channel)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from the passed channel

        :param channel: channel name to subscribe to
        :type channel: str
        """
        self.logger.info("Unsubscribing from '%s'", channel)
        if (subscription := self._channels.get(channel)) is not None:
            self.logger.debug("Cancelling task for '%s' channel", channel)
            self.logger.debug("Task %s", subscription)
            subscription.cancel()
            with suppress(asyncio.CancelledError):
                self.logger.debug("Waiting for '%s' reader task to cancel...", channel)
                del self._channels[channel]  # signal for the reader to close
                await subscription  # wait for the reader to close
        else:
            self.logger.warning("Unknown channel '%s'", channel)

    async def publish(self, channel: str, message: Any) -> None:
        """Using the configured redis connection to publish a message to the provided channel

        :param channel: Channel name to use to publish the message to
        :type channel: str
        :param message: Data that is to be published to the channel
        :type message: Any
        """
        fields = {"message": message}
        self.logger.debug("Publishing message '%s' to channel '%s'", message, channel)
        await self._redis.xadd(channel, fields)

    async def _reader(self, channel: str) -> None:
        """Read data from the passed channel object and put events into a queue to be read by the caller.

        :param channel: The channel subscribed to
        :type channel: str
        """
        self.logger.debug("Reader starting for '%s' channel", channel)
        streams: dict[str, int] = {channel: 0}
        if latest_message_id := self._channel_latest_id.get(
            channel
        ):  # history was called before
            streams[channel] = latest_message_id
        else:
            results = await self._redis.xrevrange(
                channel, count=1
            )  # read the last events from the channel
            for message_id, values in reversed(results):
                streams[channel] = message_id

        self.logger.debug(
            "Latest stream id '%s' for '%s' channel", streams[channel], channel
        )

        while True:
            response = await self._redis.xread(streams, block=100, count=1)
            self.logger.debug(
                "Read %d messages for channel '%s'", len(response), channel
            )
            for stream_name, messages in response:
                for message_id, values in messages:
                    self.logger.debug(
                        "Read message %s from channel '%s', Updating latest stream id '%s'",
                        values["message"],
                        stream_name,
                        message_id,
                    )
                    streams[stream_name] = message_id
                    event = Event(channel=stream_name, message=values["message"])
                    self._listen_queue.put_nowait(event)
            if (
                channel not in self._channels
            ):  # check if the channel has unsubscribed and break out
                break

    async def next_event(self) -> Union[Event, None]:
        """Return the next event from the queue that was read from all channels"""
        self.logger.debug("Getting next event from redis...")
        return await self._listen_queue.get()

    async def history(self, channel: str, count: int = 0) -> AsyncIterator[Event]:
        """Optionally get a history of the last `n` events

        :return: A list of the last events
        :type: List
        """
        if count > 0:
            self.logger.info("Reading the last %s events from '%s'", count, channel)
            results = await self._redis.xrevrange(
                channel, count=count
            )  # read the last `n` events from the channel
            for message_id, values in reversed(results):
                yield Event(channel=channel, message=values["message"])
                self._channel_latest_id[channel] = message_id
