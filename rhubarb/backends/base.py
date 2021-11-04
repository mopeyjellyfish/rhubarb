from typing import Any

from abc import ABC, abstractmethod

from rhubarb.event import Event


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

        :return:
        :rtype: Event
        """
