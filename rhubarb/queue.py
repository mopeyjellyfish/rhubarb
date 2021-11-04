from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Dict,
    Optional,
    Set,
    Union,
)

import asyncio
import json
import logging
from contextlib import asynccontextmanager, suppress

from .backends.redis import RedisBackend
from .event import Event


class Unsubscribed(Exception):
    pass


class Rhubarb:
    """An event bus class that abstracts access to different backend queues into a single ``asyncio.Queue``.
    This allows for updating the backend implementation of where events are subscribed from and published to without needing to change the code within each service that uses the queue.

    Has a simple to use interface that can support each combination of single and multi consumer patterns, as well as single and multi producer patterns.

    An example of using the class as a consumer of the event bus

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL) as events:
            async with events.subscribe(channel="PUBLIC_STATE_ROOM_1") as subscriber:
                async for event in subscriber:
                    await websocket.send_text(event.message)

    Its also possible to use the same class as a producer of events on the event bus

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL) as events:
            await events.publish(channel="INPUT_ROOM_1", message=json.dumps(data))

    It is also possible to parse messages with your own callable:

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL) as events:
            async with events.subscribe(channel="PUBLIC_STATE_ROOM_1", message_loader=json.loads) as subscriber:
                async for event in subscriber:
                    await websocket.send_json(event.message)

    In addition to parsing, it is also possible to publish JSON messages:

    .. code-block:: python

        async with Rhubarb(EVENT_QUEUE_URL) as events:
            await events.publish_json(channel="INPUT_ROOM_1", message=data)

    """

    def __init__(self, url: str):
        """Constructor for the event bus that configures the event bus
        `_backend`` is intended to be replaced with different backends where required.

        :param url: URL for the backend service
        :type url: str
        """
        self.logger = logging.getLogger()
        self._backend: RedisBackend = RedisBackend(
            url
        )  # This can be updated to allow for different backendsUpdate this to allow for different backends
        self._subscribers: dict[str, set[asyncio.Queue[Union[Event, None]]]] = {}
        self._lock = asyncio.Lock()

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
            await self._backend.disconnect()

    async def __aenter__(self) -> "Rhubarb":
        """Allows for the queue to be used in a context manager"""
        await self.connect()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        """Disconnect the backend when exiting the context manager"""
        await self.disconnect()

    async def publish(self, channel: str, message: str) -> None:
        """Publishes a message to the channel provided

        :param channel: channel name
        :type channel: str
        :param message: message to publish to the provided channel name
        :type message: str
        """
        self.logger.debug("Publishing to %s message: %s", channel, message)
        await self._backend.publish(channel, message)

    async def publish_json(self, channel: str, obj: Any) -> None:
        """Serializes the message into a JSON string and publishes.

        :param channel: channel name
        :type channel: str
        :param obj: message to publish to the provided channel name
        :type obj: any
        """
        message = json.dumps(obj)
        await self.publish(channel, message)

    async def _subscribe(
        self, channel: str, queue: asyncio.Queue[Union[Event, None]]
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
            if not self._subscribers.get(
                channel
            ):  # if channel doesn't exist or the set is empty then subscribe on the backend
                await self._backend.subscribe(channel)
                self._subscribers[channel] = {queue}
            else:
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
            self._subscribers[channel].remove(queue)
            if not self._subscribers.get(
                channel
            ):  # If there are no subscribers then remove channel
                del self._subscribers[channel]
                await self._backend.unsubscribe(channel)

    @asynccontextmanager
    async def subscribe(
        self, channel: str, message_loader: Optional[Callable[[str], Any]] = None
    ) -> AsyncIterator["Subscriber"]:
        """A context manager that will yield a subscriber

        :param channel: channel name to subscribe to
        :type channel: str
        """
        queue: asyncio.Queue[Union[Event, None]] = asyncio.Queue()

        try:
            await self._subscribe(channel, queue)
            yield Subscriber(queue, message_loader)
            await self._unsubscribe(channel, queue)
        finally:
            self.logger.debug("Closing queue for %s channel", channel)
            await queue.put(None)


class Subscriber:
    def __init__(
        self,
        queue: asyncio.Queue[Union[Event, None]],
        loader: Optional[Callable[[str], Any]] = None,
    ) -> None:
        """Wraps an ``asyncio.Queue`` to allow the caller to asynchronously iterate over the queue

        :param queue: `asyncio.Queue`` to will contain events for the caller to iterate over
        :type queue: asyncio.Queue
        """
        self._queue = queue
        self.loader = loader

    async def __aiter__(self) -> Optional[AsyncGenerator[Event, None]]:
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

        if self.loader:
            return Event(channel=item.channel, message=self.loader(item.message))

        return item
