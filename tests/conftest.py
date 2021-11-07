from typing import Generator

import asyncio
from asyncio.events import AbstractEventLoop

from pytest import fixture


@fixture
def REDIS_URL():
    return "redis://localhost:6379/0"


@fixture
def KAFKA_URL():
    return "kafka://localhost:9092"


@fixture
def MEMORY_URL():
    return "memory://"


@fixture(params=["redis://localhost:6379/0", "kafka://localhost:9092", "memory://"])
def URL(request):
    return request.param


@fixture(scope="session", autouse=True)
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    """Return the running event loop.

    The event loop is closed after use.
    """
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
