from typing import Any

from dataclasses import dataclass


@dataclass
class Event:
    channel: str
    message: Any

    def __repr__(self) -> str:
        return f"Event({self.channel!r}, {self.message!r})"
