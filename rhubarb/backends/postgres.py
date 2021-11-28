from typing import Any, Optional, Union

import asyncio
import logging
from logging import Logger

import asyncpg

from rhubarb.backends.base import BaseBackend
from rhubarb.event import Event


class AsyncPgBackend(BaseBackend):
    def __init__(self, url: str):
        """Backend using asyncpg

        :param url: URL for the postgres instance
        :type url: str
        """
        self.url = url
        self.logger: Logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        """Connect to the postgres instance using ``self.url``"""
        self.logger.info("Connecting to %s", self.url)
        self._connection = await asyncpg.connect(self.url)
        self._listen_queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()
        self._channels: set[str] = set()
        self.logger.info("Connected to %s", self.url)

    async def disconnect(self) -> None:
        """Disconnect from the postgres instance"""
        self.logger.info("Disconnecting from %s", self.url)
        await self._connection.close()
        self.logger.info("Disconnected from %s", self.url)

    async def subscribe(self, channel: str) -> None:
        """Subscribe to the named channel

        :param channel: the channel to subscribe to
        :type channel: str
        """
        self.logger.info("Subscribing to '%s'", channel)
        await self._connection.add_listener(channel, self._listener)
        self._channels.add(channel)
        self.logger.info("Subscribed to '%s'", channel)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from the named channel

        :param channel: the channel to unsubscribe to
        :type channel: str
        """
        self.logger.info("Unsubscribing from '%s'", channel)
        if channel in self._channels:
            await self._connection.remove_listener(channel, self._listener)
            self._channels.remove(channel)
        else:
            self.logger.warning("Unknown channel '%s'", channel)

        self.logger.info("Unsubscribed from '%s'", channel)

    async def publish(self, channel: str, message: str) -> None:
        """Publish the passed ``message`` to the provided ``channel``

        :param channel: the channel to publish the ``message`` to.
        :type channel: str
        :param message: the message to publish
        :type message: str
        """
        self.logger.debug("Publishing %s to '%s'", message, channel)
        await self._connection.execute(
            "SELECT pg_notify($1, $2);", channel, str(message)
        )

    def _listener(self, *args: Any) -> None:
        """listener callback for the asyncpg `add_listener` call"""
        _, __, channel, payload = args
        self.logger.debug(
            "Read message %s from channel: %s",
            payload,
            channel,
        )
        event = Event(channel=channel, message=payload)
        self._listen_queue.put_nowait(event)

    async def next_event(self) -> Optional[Event]:
        """Get the next event from the queue"""
        return await self._listen_queue.get()
