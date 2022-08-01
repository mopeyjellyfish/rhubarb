import asyncio
from asyncio.events import AbstractEventLoop
from collections.abc import Generator

import pytest_asyncio
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


@fixture
def POSTGRES_URL():
    return "postgres://postgres:postgres@localhost:5432/rhubarb"


@fixture
def RABBIT_MQ_URL():
    return "amqp://guest:guest@localhost/"


@fixture(
    params=[
        "redis://localhost:6379/0",
        "kafka://localhost:9092",
        "memory://",
        "postgres://postgres:postgres@localhost:5432/rhubarb",
        "amqp://guest:guest@localhost/",
    ]
)
def URL(request):
    return request.param


@pytest_asyncio.fixture(scope="session", autouse=True)
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    """Return the running event loop.

    The event loop is closed after use.
    """
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
