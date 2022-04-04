from typing import Any, AsyncIterator, List, Union

import asyncio
from abc import ABC, abstractmethod

from rhubarb.event import Event

from .exceptions import HistoryError


class BaseBackend(ABC):
    @abstractmethod
    def __init__(self, url: str) -> None:
        """
        :param url: URL of the backend service
        :type url: str
        """

    @abstractmethod
    async def connect(self) -> None:
        """Connect to queue service"""

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from queue service"""

    @abstractmethod
    async def subscribe(self, channel: str) -> None:
        """Subscribe to a channel

        :param channel: name of the channel in the queue to subscribe to
        :type channel: str
        """

    @abstractmethod
    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel

        :param channel: name of the channel in the queue to subscribe to
        :type channel: str
        """

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    @abstractmethod
    async def publish(self, channel: str, message: Any) -> None:
        """Publish a message to a given channel

        :param channel: name of the channel in the queue to publish the message to
        :type channel: str
        :param message: the message to publish to the channel
        :type channel: str
        """

    @abstractmethod
    async def next_event(self) -> Event:
        """Get the next published Event from the queue

        :return: The next event read
        :rtype: Event
        """

    async def history(
        self, channel: str, count: int = 0
    ) -> AsyncIterator[Union[Event, None]]:
        """Optionally get a history of the last `n` events

        :return: A list of the last events
        :type: List
        """
        yield None
        raise HistoryError("History not supported for backend")
