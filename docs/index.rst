Welcome to Rhubarb's documentation!
===================================

.. image:: https://github.com/mopeyjellyfish/rhubarb/workflows/build/badge.svg?branch=main&event=push
    :target: https://github.com/mopeyjellyfish/rhubarb/actions?query=workflow%3Abuild
    :alt: |Build status|
.. image:: https://img.shields.io/pypi/pyversions/rhubarb-py.svg
    :target: https://pypi.org/project/rhubarb-py
    :alt: |Python Version|
.. image:: https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg
    :target: https://github.com/mopeyjellyfish/rhubarb/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot
    :alt: |Dependencies Status|
.. image:: https://codecov.io/gh/mopeyjellyfish/rhubarb/branch/main/graph/badge.svg?token=E8F5LMKDBK
    :target: https://codecov.io/gh/mopeyjellyfish/rhubarb
    :alt: |Coverage|
.. image:: https://readthedocs.org/projects/rhubarb-py/badge/?version=latest
    :target: https://rhubarb-py.readthedocs.io/en/latest/?badge=latest
    :alt: |Documentation Status|
.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black
    :alt: |Code style: black|
.. image:: https://img.shields.io/badge/security-bandit-green.svg
    :target: https://github.com/PyCQA/bandit
    :alt: |Security: bandit|
.. image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
    :target: https://github.com/mopeyjellyfish/rhubarb/blob/master/.pre-commit-config.yaml
    :alt: |Pre-commit|
.. image:: https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg
    :target: https://github.com/mopeyjellyfish/rhubarb/releases
    :alt: |Semantic Versions|
.. image:: https://img.shields.io/github/license/mopeyjellyfish/rhubarb
    :target: https://github.com/mopeyjellyfish/rhubarb/blob/master/LICENSE
    :alt: |License|


Installation
============

.. code-block:: bash

   pip install -U rhubarb-py

or install with `Poetry`

.. code-block:: bash

   poetry add rhubarb-py

Example
=======

A minimal working example can be found in `example <https://github.com/mopeyjellyfish/rhubarb/blob/main/example/app.py>`_

.. code-block:: python

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



.. toctree::
   :maxdepth: 2
   :caption: Contents:

   source/modules



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
