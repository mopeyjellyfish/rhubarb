from typing import Any, Optional, Union

import asyncio
import logging
from logging import Logger

from rhubarb.backends.base import BaseBackend
from rhubarb.event import Event


class MemoryBackend(BaseBackend):
    def __init__(self, url: str):
        """A simple memory base queue, useful for testing and internal process communication

        :param url: Not used.
        :type url: str
        """
        self._channels: set[str] = set()
        self._url: str = url
        self.logger: Logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        """Connect simply creates the consumer queue"""
        self.logger.info("Creating queue...")
        self._consumer: asyncio.Queue[Union[Event, None]] = asyncio.Queue()
        self.logger.info("Created queue!")

    async def disconnect(self) -> None:
        """Do nothing as there is nothing to disconnect from"""
        pass

    async def subscribe(self, channel: str) -> None:
        """Add the channel to the subscribed set

        :param channel: the channel to subscribe to
        :type channel: str
        """
        self.logger.info("Subscribing to '%s'", channel)
        self._channels.add(channel)
        self.logger.info("Subscribed to '%s'", channel)

    async def unsubscribe(self, channel: str) -> None:
        """Remove the channel to the subscribed set

        :param channel: the channel to subscribe to
        :type channel: str
        """
        self.logger.info("Unsubscribing from '%s'", channel)
        if channel in self._channels:
            self._channels.remove(channel)
        else:
            self.logger.warning("Unknown channel '%s'", channel)

    async def publish(self, channel: str, message: Any) -> None:
        """Create an ``Event`` and put onto the consumer queue

        :param channel: the channel to subscribe to
        :type channel: str
        :param message: the message to publish
        :type message: str
        """
        self.logger.debug("Publishing message %s to channel '%s'", message, channel)
        event = Event(channel=channel, message=message)
        await self._consumer.put(event)

    async def next_event(self) -> Optional[Event]:
        """Get the next event from the consumer queue"""
        self.logger.debug("Getting next event...")
        while True:
            event: Optional[Event] = await self._consumer.get()
            if event and event.channel in self._channels:
                return event
