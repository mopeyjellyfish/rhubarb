from typing import Any, AsyncIterator, List, Union

import asyncio
import logging
from collections import namedtuple
from contextlib import suppress
from logging import Logger
from time import sleep

import aioredis

from rhubarb.backends.base import BaseBackend
from rhubarb.event import Event
from tests.test_rhubarb import queue

GroupConsumer = namedtuple("GroupConsumer", "group consumer")


class RedisBackend(BaseBackend):
    def __init__(self, url: str):
        """Backend for the message queue using ``aioredis`` to interface with redis

        :param url: The URL of the redis instance
        :type url: str
        """
        self.url: str = url
        self._channels: dict[str, asyncio.Task[None]] = {}  # the channels subscribed to
        self._group_readers: dict[
            str, dict[str, asyncio.Task[None]]
        ] = {}  # hanndles many group readers
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

        for channel in list(self._group_readers.keys()):
            for consumer in list(self._group_readers[channel].keys()):
                await self.group_unsubscribe(
                    channel, consumer.group, consumer.consumer
                )  # pragma: no cover

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

    async def _group_reader(
        self,
        channel: str,
        group_consumer: GroupConsumer,
        queue: asyncio.Queue[Union[Event, None]],
    ):
        """A group reader will take a channel and group_consumer to consume messages that are put onto the queue
        allowing for an at-most-once delivery of messages for a group of subscribers.

        :param channel: The channel subscribed to
        :type channel: str
        :param group_consumer: The channel subscribed to
        :type group_consumer: GroupConsumer
        :param queue: The queue that the consumer is reading from
        :type queue: asyncio.Queue
        """
        self.logger.info(
            "Group Reader starting for channel '%s' group: '%s' consumer name: '%s'",
            channel,
            group_consumer.group,
            group_consumer.consumer,
        )
        streams: dict[str, int] = {channel: ">"}
        while True:
            response = await self._redis.xreadgroup(
                group_consumer.group, group_consumer.consumer, streams=streams, count=1
            )
            for stream_name, messages in response:
                self.logger.debug(
                    "Read %d messages for channel '%s' group '%s' consumer '%s'",
                    len(messages),
                    stream_name,
                    group_consumer.group,
                    group_consumer.consumer,
                )
                for message_id, values in messages:
                    self.logger.debug(
                        "Read message %s from channel '%s' Group '%s'  Consumer '%s', stream id '%s'",
                        values["message"],
                        stream_name,
                        group_consumer.group,
                        group_consumer.consumer,
                        message_id,
                    )
                    event = Event(channel=stream_name, message=values["message"])
                    queue.put_nowait(event)
                    self.logger.debug(
                        "Stream '%s' Consumer '%s' Group '%s' Waiting for processing... %s",
                        stream_name,
                        group_consumer.consumer,
                        group_consumer.group,
                        id(queue),
                    )
                    await queue.join()
                    self.logger.debug(
                        "Stream '%s' Consumer '%s' Group '%s' Queue finished... %s",
                        stream_name,
                        group_consumer.consumer,
                        group_consumer.group,
                        id(queue),
                    )
            if group_consumer not in self._group_readers.get(
                channel
            ):  # check if the group consumer has unsubscribed and break out
                break  # pragma: no cover
            else:
                await asyncio.sleep(0)

    async def group_subscribe(
        self,
        channel: str,
        group_name: str,
        consumer_name: str,
        queue: asyncio.Queue[Union[Event, None]],
    ):
        """Called to subscribe to a channel as part of a consumer (``consumer_name``) within a group (``groupd_name``)

        :param channel: name of the channel in the queue to subscribe to
        :type channel: str
        :param group_name: the name of the group this subscriber will join
        :type group_name: str
        :param consumer_name: the unique name in the group for this subscriber
        :type consumer_name: str
        """
        self.logger.info(
            "Subscribing to '%s' with group '%s' consumer '%s'",
            channel,
            group_name,
            consumer_name,
        )
        try:
            await self._redis.xgroup_create(
                name=channel, groupname=group_name, mkstream=True
            )
        except aioredis.ResponseError as e:
            if str(e) != "BUSYGROUP Consumer Group name already exists":
                raise  # pragma: no cover

        self.logger.info(
            "Creating reader task on channel '%s' for group '%s' consumer '%s'",
            channel,
            group_name,
            consumer_name,
        )
        if channel not in self._group_readers:
            self._group_readers[channel] = {}

        group_consumer = GroupConsumer(group_name, consumer_name)

        if group_consumer in self._group_readers[channel]:
            self.logger.warning(
                "Consumer %s is already reading in group %s", consumer_name, group_name
            )  # pragma: no cover

        self._group_readers[channel][group_consumer] = asyncio.create_task(
            self._group_reader(channel, group_consumer, queue)
        )
        self.logger.info(
            "Subscribed to '%s' in group '%s' as consumer '%s'",
            channel,
            group_name,
            consumer_name,
        )

    async def group_unsubscribe(
        self, channel: str, group_name: str, consumer_name: str
    ):
        """Called to unsubscribe from a channel based on the group name and consumer name

        :param channel: name of the channel in the queue to subscribe to
        :type channel: str
        :param group_name: the name of the group this subscriber will join
        :type group_name: str
        :param consumer_name: the unique name in the group for this subscriber
        :type consumer_name: str
        """
        self.logger.info("Unsubscribing from '%s'", channel)
        if (channel := self._group_readers.get(channel)) is not None:
            self.logger.debug(
                "Cancelling task for '%s' channel group '%s' consumer '%s'",
                channel,
                group_name,
                consumer_name,
            )
            group_consumer = GroupConsumer(group_name, consumer_name)
            if (task := channel.get(group_consumer)) is not None:
                self.logger.debug("Task %s", task)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    self.logger.debug("Waiting for '%s' reader task to cancel...", task)
                    del channel[group_consumer]  # signal for the reader to close
                    await task  # wait for the reader to close
            else:
                self.logger.warning(
                    "Unknown group '%s' consumer '%s' combination for channel '%s'",
                    group_name,
                    consumer_name,
                    channel,
                )
        else:
            self.logger.warning("Unknown channel '%s'", channel)
