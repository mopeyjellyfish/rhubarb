# Rhubarb

<div align="center">

[![Build status](https://github.com/mopeyjellyfish/rhubarb/workflows/build/badge.svg?branch=main&event=push)](https://github.com/mopeyjellyfish/rhubarb/actions?query=workflow%3Abuild)
[![Python Version](https://img.shields.io/pypi/pyversions/rhubarb-py.svg)](https://pypi.org/project/rhubarb-py)
[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/mopeyjellyfish/rhubarb/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)
[![codecov](https://codecov.io/gh/mopeyjellyfish/rhubarb/branch/main/graph/badge.svg?token=E8F5LMKDBK)](https://codecov.io/gh/mopeyjellyfish/rhubarb)
[![Documentation Status](https://readthedocs.org/projects/rhubarb-py/badge/?version=latest)](https://rhubarb-py.readthedocs.io/en/latest/?badge=latest)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/mopeyjellyfish/rhubarb/blob/master/.pre-commit-config.yaml)
[![Semantic Versions](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg)](https://github.com/mopeyjellyfish/rhubarb/releases)
[![License](https://img.shields.io/github/license/mopeyjellyfish/rhubarb)](https://github.com/mopeyjellyfish/rhubarb/blob/master/LICENSE)

Rhubarb is a library that simplifies realtime streaming of events for a number of backends in to a single API. Currently supports [`Postgres`](https://github.com/MagicStack/asyncpg), [`kafka`](https://github.com/aio-libs/aiokafka), [`RabbitMQ`](https://github.com/mosquito/aio-pika), [`redis`](https://github.com/aio-libs/aioredis-py) as well as an internal memory backend useful for testing.

</div>

## Installation

There are a number of backends that can be used with Rhubarb:

| Kafka | Postgres | Redis | RabbitMQ |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |--------------------------------------------------------------------------------- |
| <p align="center"><img src="./README_assets/kafka.png" width="60" height="100"/></p>    | <p align="center"><img src="./README_assets/postgres.png" width="100" height="100" /></p> | <p align="center"><img src="./README_assets/redis.png" width="100" height="80"/></p> | <p align="center"><img src="./README_assets/rabbitmq.jpg" width="100" height="100" /></p> |
| `pip install rhubarb-py[kafka]` | `pip install rhubarb-py[postgres]` | `pip install rhubarb-py[redis]` | `pip install rhubarb-py[rabbitmq]` |

## Backends

- `Rhubarb("redis://localhost:6379/0")`
- `Rhubarb("kafka://localhost:9092")`
- `Rhubarb("postgres://postgres:postgres@localhost:5432/rhubarb")`
- `Rhubarb("amqp://guest:guest@localhost/")`
- `Rhubarb("memory://")`

## Quick start

### Simple event consumer

```python
async with Rhubarb("redis://localhost:6379/0") as events:
    async with events.subscribe(channel="CHATROOM") as subscriber:
        async for event in subscriber:
            await websocket.send_text(event.message)
```

### Simple event producer

```python
async with Rhubarb("redis://localhost:6379/0") as events:
    await events.publish("test message")
```

### History retrieval

```python
async with Rhubarb("redis://localhost:6379/0") as events: 
    async with events.subscribe(channel="CHATROOM", history=10) as subscriber: # read the last 10 events published to the channel
        async for event in subscriber:
            await websocket.send_text(event.message)
```

### Custom serializer & deserializer

```python
async with Rhubarb("redis://localhost:6379/0", serializer=json.dumps, deserializer=json.loads) as events:
    async with events.subscribe(channel="CHATROOM", history=10) as subscriber: # read the last 10 events published to the channel
        async for event in subscriber:
            await websocket.send_text(event.message)
```

### Group subscribing (at-most-once processing)

```python
async with Rhubarb("redis://localhost:6379/0", serializer=json.dumps, deserializer=json.loads) as events:
    async with events.subscribe(
        "TEST-GROUP-CHANNEL", group_name="TEST_GROUP", consumer_name="sub_1"
    ) as subscriber_1:
        async for event in subscriber:
            await process_job(event)
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
