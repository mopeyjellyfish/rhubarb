import os

from starlette.applications import Starlette
from starlette.concurrency import run_until_first_complete
from starlette.responses import HTMLResponse
from starlette.routing import Route, WebSocketRoute

from rhubarb import Rhubarb  # type: ignore[attr-defined]

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
