# type: ignore[attr-defined]
"""Rhubarb is a library that simplifies realtime streaming for a number of backends into a single API"""

import sys
from importlib import metadata as importlib_metadata

from ._core import Rhubarb, SubscribeError, UnknownBackend, Unsubscribed
from .event import Event

__all__ = ["Rhubarb", "Event", "Unsubscribed", "UnknownBackend", "SubscribeError"]


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


version: str = get_version()
