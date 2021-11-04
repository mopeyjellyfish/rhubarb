from typing import Generator

import asyncio
from asyncio.events import AbstractEventLoop

import pytest


@pytest.fixture
def URL():
    return "redis://redis:6379/0"


@pytest.fixture(scope="session", autouse=True)
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    """Return the running event loop.

    The event loop is closed after use.
    """
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
