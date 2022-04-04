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


class SubscribeError(Exception):
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

    It's also possible to use the same class as a producer of events on the event bus

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
        :param serializer: URL for the backend service
        :type serializer: Optional[Callable[[Any], str]]
        :param deserializer: URL for the backend service
        :type deserializer: Optional[Callable[[str], Any]]
        """
        self.logger: Logger = logging.getLogger(__name__)
        backend_cls = self.get_backend(url)
        self._backend: BaseBackend = backend_cls(url)
        self._serializer: Optional[Callable[[Any], str]] = serializer
        self._deserializer: Optional[Callable[[str], Any]] = deserializer
        self._subscribers: dict[str, set[asyncio.Queue[Union[Event, None]]]] = {}
        self._lock = asyncio.Lock()
        self._connected = False

    def get_backend(self, url: str) -> BaseBackend:
        parsed_url = urlparse(url)
        self.logger.info("Loading backend for: '%s", parsed_url.scheme)
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
            if not self._connected:
                await self._backend.connect()
                self._reader_task = asyncio.create_task(self._reader())
                self._connected = True
                self.logger.info("Connected to backend!")
            else:
                self.logger.warning(
                    "Already connected '%s', was 'connect' called more than once?",
                    self._connected,
                )

    async def _close_subscriptions(self) -> None:
        """Close queues for subscribers"""
        self.logger.debug("Closing all subscriptions...")
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
            self._connected = False
        self.logger.info("Disonnected from backend")

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
        :type message: Any
        """
        _message = None
        if self._serializer:  # if a serializer has been provided then call
            self.logger.debug("Serializing message")
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
        self.logger.info(
            "Subscribing to '%s', getting %d historic events", channel, history
        )
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
        self.logger.debug("Current subscribers %s", " ".join(self._subscribers.keys()))
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
                    self.logger.debug(
                        "No more subscriptions, unsubscribing from backend."
                    )
                    del self._subscribers[channel]
                    await self._backend.unsubscribe(channel)
        self.logger.debug("Subscribed to %s", " ".join(self._subscribers.keys()))

    async def _group_unsubscribe(
        self,
        channel: str,
        queue: asyncio.Queue[Union[Event, None]],
        group_name: str,
        consumer_name: str,
    ) -> None:
        """Handles unsubscribing a single consumer from a group

        Will always call the backend to unsubscribe

        :param channel: channel name to subscribe to
        :type channel: str
        :param queue: Queue to remove from subscribers
        :type queue: asyncio.Queue
        :param group_name: the name of the group this subscriber will join
        :type group_name: str
        :param consumer_name: the unique name in the group for this subscriber
        :type consumer_name: str
        """
        self.logger.info(
            "Unsubscribing from '%s' group: '%s' consumer: '%s'",
            channel,
            group_name,
            consumer_name,
        )
        await self._backend.group_unsubscribe(
            channel=channel, group_name=group_name, consumer_name=consumer_name
        )

    async def _group_subscribe(
        self,
        channel: str,
        queue: asyncio.Queue[Union[Event, None]],
        group_name: str,
        consumer_name: str,
    ) -> asyncio.Queue[Union[Event, None]]:
        """Subscribes a consumer to a group, will request the group to be created in the backend if required.

        Will only subscribe the backend if the channel has not already been used.

        :param channel: channel name to subscribe to
        :type channel: str
        :param queue: new subscribers ``asyncio.Queue``
        :type queue: asyncio.Queue
        """
        self.logger.info(
            "Subscribing to '%s' group: '%s' consumer: '%s'",
            channel,
            group_name,
            consumer_name,
        )
        async with self._lock:  # ensure that only one co-routine mutates state at a time
            await self._backend.group_subscribe(
                channel, group_name, consumer_name, queue
            )

    @asynccontextmanager
    async def subscribe(
        self,
        channel: str,
        history: Optional[int] = 0,
        group_name: Optional[str] = None,
        consumer_name: Optional[str] = None,
    ) -> AsyncIterator["Subscriber"]:
        """A context manager that will yield a subscriber

        :param channel: channel name to subscribe to
        :type channel: str
        :param history: the number of historical events to retrieve, defaults to 0
        :type history: int
        :param group_name: the name of the group this subscriber will join
        :type group_name: str
        :param consumer_name: the unique name in the group for this subscriber
        :type consumer_name: str
        """
        if (group_name or consumer_name) and (
            group_name is None or consumer_name is None
        ):
            raise SubscribeError(f"Must specify both 'group_name' and 'consumer_name'")

        group_subscribe = group_name is not None and consumer_name is not None

        queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()
        try:
            # branch here to create a different set of group subscribers, keyed on the group and consumer name
            if group_subscribe:
                self.logger.debug(
                    "Group '%s' subscription to '%s' queue: '%s'",
                    group_name,
                    channel,
                    id(queue),
                )
                await self._group_subscribe(channel, queue, group_name, consumer_name)
            else:
                await self._subscribe(channel, queue, history)

            yield Subscriber(queue, self._deserializer)
        finally:
            # handle clean up for group subscriber vs fan out subscriber
            if group_subscribe:
                await self._group_unsubscribe(channel, queue, group_name, consumer_name)
            else:
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
                self._queue.task_done()
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
