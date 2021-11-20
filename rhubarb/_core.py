from typing import Any, AsyncGenerator, AsyncIterator, Callable, Optional, Union

import asyncio
import json
import logging
from contextlib import asynccontextmanager, suppress
from logging import Logger
from urllib.parse import urlparse

from .backends.base import BaseBackend
from .event import Event


class Unsubscribed(Exception):
    pass


class UnknownBackend(Exception):
    pass


class Rhubarb:
    """An event bus class that abstracts access to different backend queues into a single ``asyncio.Queue``.
    This allows for updating the backend implementation of where events are subscribed from and published to without needing to change the code within each service that uses the queue.

    Has a simple to use interface that can support each combination of single and multi consumer patterns, as well as single and multi producer patterns.

    An example of using the class as a consumer of the event bus

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL) as events:
            async with events.subscribe(channel="CHATROOM") as subscriber:
                async for event in subscriber:
                    await websocket.send_text(event.message)

    Its also possible to use the same class as a producer of events on the event bus

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL) as events:
            await events.publish(channel="CHATROOM", message=json.dumps(data))

    It is also possible to parse messages with your own callable:

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL, serializer=json.loads, deserializer=json.dumps) as events:
            async with events.subscribe(channel="CHATROOM") as subscriber:
                async for event in subscriber:
                    await websocket.send_json(event.message)

    In addition to parsing, it is also possible to publish JSON messages:

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL, serializer=json.loads, deserializer=json.dumps) as events:
            await events.publish(channel="CHATROOM", message=data)

    """

    def __init__(
        self,
        url: str,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[str], Any]] = None,
    ):
        """Constructor for the event bus that configures the event bus
        `_backend`` is intended to be replaced with different backends where required.

        :param url: URL for the backend service
        :type url: str
        """
        backend_cls = self.get_backend(url)
        self.logger: Logger = logging.getLogger(__name__)
        self._backend: BaseBackend = backend_cls(url)
        self._serializer: Optional[Callable[[Any], str]] = serializer
        self._deserializer: Optional[Callable[[str], Any]] = deserializer
        self._subscribers: dict[str, set[asyncio.Queue[Union[Event, None]]]] = {}
        self._lock = asyncio.Lock()

    def get_backend(self, url: str) -> BaseBackend:
        parsed_url = urlparse(url)

        if parsed_url.scheme == "redis":
            from rhubarb.backends.redis import RedisBackend

            return RedisBackend
        elif parsed_url.scheme == "kafka":
            from rhubarb.backends.kafka import KafkaBackend

            return KafkaBackend
        elif parsed_url.scheme == "postgres":
            from rhubarb.backends.postgres import AsyncPgBackend

            return AsyncPgBackend
        elif parsed_url.scheme == "memory":
            from rhubarb.backends.memory import MemoryBackend

            return MemoryBackend
        elif parsed_url.scheme == "amqp":
            from rhubarb.backends.rabbitmq import RabbitMQBackend

            return RabbitMQBackend
        else:
            raise UnknownBackend(f'"{parsed_url.scheme}" is not a supported backend!')

    async def _reader(self) -> None:
        """Reads the backend queue and passes events to Subscribers"""
        self.logger.info("Started reading backend events")
        while True:
            event = await self._backend.next_event()
            self.logger.debug("Read new event from backend")
            for queue in self._subscribers.get(
                event.channel, []
            ):  # publish event to all subscribers for this event
                await queue.put(event)

    async def connect(self) -> None:
        """Connect to backend and read Events from the queue"""
        self.logger.info("Connecting to backend")
        async with self._lock:
            await self._backend.connect()
            self._reader_task = asyncio.create_task(self._reader())

    async def _close_subscriptions(self) -> None:
        """Close queues for subscribers"""
        for subscribers in self._subscribers.values():
            for queue in subscribers:
                await queue.put(None)

    async def disconnect(self) -> None:
        """Close the running backend reader and ensure that the backend is disconnected"""
        self.logger.info("Disconnecting from backend")
        async with self._lock:
            if self._reader_task.done():
                with suppress(asyncio.exceptions.CancelledError):
                    self._reader_task.result()
            else:
                self._reader_task.cancel()
                with suppress(asyncio.exceptions.CancelledError):
                    await self._reader_task

            await self._close_subscriptions()  # close subscriptions so that subscribers to the event bus stop waiting for events
            await self._backend.disconnect()

    async def __aenter__(self) -> "Rhubarb":
        """Allows for the queue to be used in a context manager"""
        await self.connect()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        """Disconnect the backend when exiting the context manager"""
        await self.disconnect()

    async def publish(self, channel: str, message: Any) -> None:
        """Publishes a message to the channel provided

        :param channel: channel name
        :type channel: str
        :param message: message to publish to the provided channel name
        :type message: str
        """
        _message = None
        if self._serializer:  # if a serializer has been provided then call
            _message = self._serializer(message)
        else:
            _message = message

        self.logger.debug("Publishing to %s message: %s", channel, _message)
        await self._backend.publish(channel, _message)

    async def _get_history(
        self, channel: str, queue: asyncio.Queue[Union[Event, None]], count: int = 0
    ) -> None:
        """Get the last `n` events from the backend

        :param channel: channel name to retrieve the history of
        :type channel: str
        :param queue: the queue to write historical events to
        :type queue: asyncio.Queue
        :param count: number of events to retrieve from the history
        :type count: int
        """
        self.logger.info("Retrieving %s events from channel '%s'", count, channel)
        if count > 0:
            async for event in self._backend.history(channel, count=count):
                queue.put_nowait(event)

    async def _subscribe(
        self, channel: str, queue: asyncio.Queue[Union[Event, None]], history: int = 0
    ) -> asyncio.Queue[Union[Event, None]]:
        """Subscribes a queue to the channel and subscribes to the channel in the backend

        Will only subscribe the backend if the channel has not already been used.

        :param channel: channel name to subscribe to
        :type channel: str
        :param queue: new subscribers ``asyncio.Queue``
        :type queue: asyncio.Queue
        """
        self.logger.debug("Subscribing to %s", channel)
        async with self._lock:  # ensure that only one co-routine mutates state at a time
            await self._get_history(channel, queue, count=history)

            if not self._subscribers.get(
                channel
            ):  # if channel doesn't exist or the set is empty then subscribe on the backend
                self.logger.debug(
                    "New subscription, subscribing to '%s' on the backend", channel
                )
                await self._backend.subscribe(channel)
                self._subscribers[channel] = {queue}
            else:
                self.logger.debug(
                    "Already subscribed to '%s', appending queue", channel
                )
                self._subscribers[channel].add(queue)

        return queue

    async def _unsubscribe(
        self, channel: str, queue: asyncio.Queue[Union[Event, None]]
    ) -> None:
        """Unsubscribe from the channel

        Will only unsubscribe the backend if the channel is no longer used.

        :param channel: channel name to subscribe to
        :type channel: str
        :param queue: Queue to remove from subscribers
        :type queue: asyncio.Queue
        """
        self.logger.debug("Unsubscribing from %s", channel)
        async with self._lock:  # ensure that only one co-routine mutates state at a time
            if (subscriber := self._subscribers.get(channel)) is not None:
                subscriber.remove(queue)
                if not subscriber:
                    del self._subscribers[channel]
                    await self._backend.unsubscribe(channel)

    @asynccontextmanager
    async def subscribe(
        self, channel: str, history: int = 0
    ) -> AsyncIterator["Subscriber"]:
        """A context manager that will yield a subscriber

        :param channel: channel name to subscribe to
        :type channel: str
        :param history: the number of historical events to retrieve, defaults to 0
        :type history: int
        """
        queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()

        try:
            await self._subscribe(channel, queue, history)
            yield Subscriber(queue, self._deserializer)
        finally:
            await self._unsubscribe(channel, queue)
            self.logger.debug("Closing queue for %s channel", channel)
            await queue.put(None)


class Subscriber:
    def __init__(
        self,
        queue: asyncio.Queue[Union[Event, None]],
        deserializer: Optional[Callable[[str], Any]] = None,
    ) -> None:
        """Wraps an ``asyncio.Queue`` to allow the caller to asynchronously iterate over the queue

        :param queue: `asyncio.Queue`` to will contain events for the caller to iterate over
        :type queue: asyncio.Queue
        """
        self._queue = queue
        self._deserializer = deserializer

    async def __aiter__(self) -> AsyncGenerator[Event, None]:
        """Async iterable allows for iterating over subscribed events"""
        try:
            while event := await self.get():
                yield event
        except Unsubscribed:
            pass

    async def get(self) -> Event:
        """Get the next item from the subscribers ``_queue``

        :raises: Unsubscribed when the queue is empty
        :returns: Event
        """
        item = await self._queue.get()
        if item is None:
            raise Unsubscribed()

        if self._deserializer:
            return Event(channel=item.channel, message=self._deserializer(item.message))

        return item
