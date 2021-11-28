from typing import Any, List, Optional, Set, Union

import asyncio
import logging
from contextlib import suppress
from logging import Logger
from urllib.parse import urlparse

import aio_pika

from rhubarb.backends.base import BaseBackend
from rhubarb.event import Event


class RabbitMQBackend(BaseBackend):
    def __init__(self, url: str):
        """`aio_pika` backend for RabbitMQ

        :param url: the kafka URL to connect to
        :type url: str
        """
        self._url: str = url
        self._channels: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        self.logger: Logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        """Connects the producer to kafka backend"""
        self.logger.info("Connecting to %s", self._url)
        self._listen_queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()
        self._connection: Any = await aio_pika.connect_robust(self._url)
        self._producer = await self._connection.channel()
        self.logger.info("Connected to %s", self._url)

    async def disconnect(self) -> None:
        """Disconnects and cleans up consumers"""
        self.logger.info("Disconnecting from %s", self._url)
        for channel in list(self._channels.keys()):
            await self.unsubscribe(channel)
        await self._connection.close()
        self.logger.info("Disconnected from %s", self._url)

    async def subscribe(self, channel: str) -> None:
        """Adds the provided channel to the dict of ``self.channels``

        :param channel: the name of the channel to subscribe to
        :type channel: str
        """
        self.logger.info("Subscribing to '%s'", channel)
        if channel not in self._channels:
            rabbit_mq_channel = await self._connection.channel()
            queue: aio_pika.Queue = await rabbit_mq_channel.declare_queue(
                channel, auto_delete=True
            )
            task: asyncio.Task[None] = asyncio.create_task(self._reader(queue))
            self._channels[channel] = task
            self.logger.info("Subscribed to '%s'", channel)

    async def unsubscribe(self, channel: str) -> None:
        """Removes the channel from the ``channels`` and re-creates the consumer

        :param channel: the channel to unsubscribe
        :type channel: str
        """
        self.logger.info("Unsubscribing from '%s'", channel)
        task = self._channels.get(channel)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            del self._channels[channel]
            self.logger.info("Unsubscribed from channel '%s'", channel)
        else:
            self.logger.warning("Unknown channel '%s'", channel)

    async def publish(self, channel: str, message: Any) -> None:
        """Using the created ``self._producer`` publish a message to the provided channel

        :param channel: Channel name to use to publish the message to
        :type channel: str
        :param message: Data that is to be published to the channel
        :type message: Any
        """
        self.logger.debug("Publishing message %s to channel %s", message, channel)
        await self._producer.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=channel,
        )

    async def _reader(self, queue: aio_pika.Queue) -> None:
        """Read data from the passed queue object and put events into a queue to be read by the caller.

        :param queue: The channel subscribed to
        :type queue: aio_pika.Queue
        """
        self.logger.debug("Reader for channel '%s' started...", queue.name)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    message = message.body.decode()
                    self.logger.debug(
                        "Read message '%s' from channel '%s'", message, queue.name
                    )
                    event = Event(channel=queue.name, message=message)
                    self._listen_queue.put_nowait(event)

    async def next_event(self) -> Union[Event, None]:
        """Return the next event from the queue that was read from all channels"""
        self.logger.debug("Getting next event from redis...")
        return await self._listen_queue.get()
