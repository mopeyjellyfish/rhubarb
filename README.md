# Rhubarb

<div align="center">

[![Build status](https://github.com/mopeyjellyfish/rhubarb/workflows/build/badge.svg?branch=main&event=push)](https://github.com/mopeyjellyfish/rhubarb/actions?query=workflow%3Abuild)
[![Python Version](https://img.shields.io/pypi/pyversions/rhubarb-py.svg)](https://pypi.org/project/rhubarb-py)
[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/mopeyjellyfish/rhubarb/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)
[![codecov](https://codecov.io/gh/mopeyjellyfish/rhubarb/branch/main/graph/badge.svg?token=E8F5LMKDBK)](https://codecov.io/gh/mopeyjellyfish/rhubarb)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/mopeyjellyfish/rhubarb/blob/master/.pre-commit-config.yaml)
[![Semantic Versions](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg)](https://github.com/mopeyjellyfish/rhubarb/releases)
[![License](https://img.shields.io/github/license/mopeyjellyfish/rhubarb)](https://github.com/mopeyjellyfish/rhubarb/blob/master/LICENSE)

Rhubarb is a library that simplifies realtime streaming of events for a number of backends in a single API

</div>

## Installation

```bash
pip install -U rhubarb-py
```

or install with `Poetry`

```bash
poetry add rhubarb-py
```

## Example

A minimal working example can be found in [example](https://github.com/mopeyjellyfish/rhubarb/blob/main/example/app.py) directory.

```python
import os

from starlette.applications import Starlette
from starlette.concurrency import run_until_first_complete
from starlette.responses import HTMLResponse
from starlette.routing import Route, WebSocketRoute

from rhubarb import Rhubarb

URL = os.environ.get("URL", "redis://localhost:6379/0")

events = Rhubarb(URL)

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://localhost:8000/ws");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""


async def homepage(_):
    return HTMLResponse(html)


async def room_consumer(websocket):
    async for message in websocket.iter_text():
        await events.publish(channel="chatroom", message=message)


async def room_producer(websocket):
    async with events.subscribe(channel="chatroom") as subscriber:
        async for event in subscriber:
            await websocket.send_text(event.message)


async def ws(websocket):
    await websocket.accept()
    await run_until_first_complete(
        (room_consumer, {"websocket": websocket}),
        (room_producer, {"websocket": websocket}),
    )


routes = [
    Route("/", homepage),
    WebSocketRoute("/ws", ws, name="chatroom_ws"),
]


app = Starlette(
    routes=routes,
    on_startup=[events.connect],
    on_shutdown=[events.disconnect],
)
```



## 🛡 License

[![License](https://img.shields.io/github/license/mopeyjellyfish/rhubarb)](https://github.com/mopeyjellyfish/rhubarb/blob/master/LICENSE)

This project is licensed under the terms of the `MIT` license. See [LICENSE](https://github.com/mopeyjellyfish/rhubarb/blob/master/LICENSE) for more details.
