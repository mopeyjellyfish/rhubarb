from typing import Any, List, Optional, Set, Union

import asyncio
import logging
from contextlib import suppress
from logging import Logger
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from rhubarb.backends.base import BaseBackend
from rhubarb.event import Event


class KafkaBackend(BaseBackend):
    def __init__(self, url: str):
        """aiokafka backend

        :param url: the kafka URL to connect to
        :type url: str
        """
        self._servers: list[str] = [urlparse(url).netloc]
        self._channels: set[str] = set()
        self._lock = asyncio.Lock()
        self._listen_queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()
        self._consumer_reader_task = None
        self.logger: Logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        """Connects the producer to kafka backend"""
        self.logger.info("Connecting to %s", self._servers)
        self._producer = AIOKafkaProducer(bootstrap_servers=self._servers)
        await self._producer.start()
        self.logger.info("Connected to %s", self._servers)

    async def disconnect(self) -> None:
        """Disconnects and cleans up consumers"""
        self.logger.info("Disconnecting from %s", self._servers)
        await self._producer.stop()
        await self._stop_consumer()
        self.logger.info("Disconnected from %s", self._servers)

    async def _start_consumer(self):
        """Internal start consumer method creating a kafka consumer for the requested channels"""
        self._consumer = AIOKafkaConsumer(
            *self._channels, bootstrap_servers=self._servers
        )
        await self._consumer.start()
        self._consumer_reader_task: asyncio.Task[None] = asyncio.create_task(  # type: ignore
            self._reader()
        )

    async def _stop_consumer(self):
        """Stops the consumer task and the AIOKafkaConsumer object"""
        if self._consumer_reader_task:
            self._consumer_reader_task.cancel()  # type: ignore
            with suppress(asyncio.exceptions.CancelledError):
                await self._consumer_reader_task
            await self._consumer.stop()

    async def subscribe(self, channel: str) -> None:
        """Adds the provided channel to the set of ``self.channels`` and restarts the consumer

        :param channel:
        :type channel: str
        """
        async with self._lock:
            await self._stop_consumer()
            self._channels.add(channel)
            await self._start_consumer()

    async def unsubscribe(self, channel: str) -> None:
        """Removes the channel from the ``channels`` and re-creates the consumer

        :param channel: the channel to unsubscribe
        :type channel: str
        """
        self.logger.info("Unsubscribing from %s", channel)
        async with self._lock:
            if channel in self._channels:
                await self._stop_consumer()
                self._channels.remove(channel)
                if self._channels:
                    await self._start_consumer()
            else:
                self.logger.warning("Unknown channel %s", channel)

    async def publish(self, channel: str, message: Any) -> None:
        """Using the created ``self._producer`` publish a message to the provided channel

        :param channel: Channel name to use to publish the message to
        :type channel: str
        :param message: Data that is to be published to the channel
        :type message: Any
        """
        self.logger.debug("Publishing message %s to channel %s", message, channel)
        await self._producer.send_and_wait(channel, message.encode("utf8"))

    async def _reader(self) -> None:
        """Read data from the consumer and put events into a queue to be read by the caller."""
        async for message in self._consumer:
            self.logger.debug(
                "Read message %s from channel: %s",
                message.value.decode("utf8"),
                message.topic,
            )
            event = Event(channel=message.topic, message=message.value.decode("utf8"))
            self._listen_queue.put_nowait(event)

    async def next_event(self) -> Optional[Event]:
        """Return the next event from the queue that was read from all channels"""
        self.logger.debug("Getting next event from kafka...")
        return await self._listen_queue.get()
