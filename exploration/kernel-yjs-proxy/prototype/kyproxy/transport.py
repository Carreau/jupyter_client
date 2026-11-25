"""Y websocket room server + client helpers.

The room server is a plain pycrdt-websocket WebsocketServer served through the
`websockets` library. It knows nothing about kernels: the proxy connects to it
exactly like a browser would.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from pycrdt import Doc, Provider
from pycrdt.websocket import WebsocketServer
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


class WSChannel:
    """Adapts a `websockets` connection to pycrdt's Channel protocol."""

    def __init__(self, ws, path: str):
        self._ws = ws
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.recv()
        except ConnectionClosed:
            raise StopAsyncIteration

    async def send(self, message: bytes) -> None:
        try:
            await self._ws.send(message)
        except ConnectionClosed:
            pass

    async def recv(self) -> bytes:
        m = await self._ws.recv()
        return m if isinstance(m, bytes) else m.encode()


class RoomServer:
    """Hosts Y rooms over websockets. Rooms are never auto-cleaned (no ystore here)."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host, self.port = host, port
        self.ws_server = WebsocketServer(auto_clean_rooms=False)

    async def _main(self):
        async with self.ws_server:
            self._srv = await serve(self._handler, self.host, self.port, max_size=None)
            self.port = self._srv.sockets[0].getsockname()[1]
            self._ready.set()
            await self._stop.wait()
            self._srv.close()
            await self._srv.wait_closed()

    async def __aenter__(self) -> "RoomServer":
        # run in a dedicated task: anyio task groups must be exited by their own task
        self._ready, self._stop = asyncio.Event(), asyncio.Event()
        self._task = asyncio.create_task(self._main())
        await self._ready.wait()
        return self

    async def __aexit__(self, *exc):
        self._stop.set()
        await asyncio.wait_for(self._task, 10)

    async def _handler(self, ws):
        await self.ws_server.serve(WSChannel(ws, ws.request.path))

    async def room_doc(self, name: str) -> Doc:
        """Server-side access to a room's doc (e.g. to load a notebook from disk)."""
        return (await self.ws_server.get_room(name)).ydoc


@asynccontextmanager
async def connect_doc(port: int, room: str, doc: Doc | None = None, host: str = "127.0.0.1"):
    """Connect a Doc to a room and wait for initial sync."""
    doc = Doc() if doc is None else doc
    async with connect(f"ws://{host}:{port}{room}", max_size=None) as ws:
        async with Provider(doc, WSChannel(ws, room)) as provider:
            await asyncio.wait_for(provider.synced.wait(), 10)
            yield doc


class DocConnection:
    """Owns a connect_doc() context in its own task, so that it can be opened
    and closed from different tasks (anyio cancel scopes must be exited in the
    task that entered them; pytest-asyncio fixtures violate that)."""

    def __init__(self, port: int, room: str):
        self.port, self.room = port, room
        self._ready = asyncio.Event()
        self._stop = asyncio.Event()

    async def _main(self):
        try:
            async with connect_doc(self.port, self.room) as doc:
                self.doc = doc
                self._ready.set()
                await self._stop.wait()
        except BaseException as e:  # surface connection errors to open()
            self._error = e
            self._ready.set()
            raise

    async def open(self) -> Doc:
        self._error = None
        self._task = asyncio.create_task(self._main())
        await self._ready.wait()
        if self._error:
            raise self._error
        return self.doc

    async def close(self):
        self._stop.set()
        try:
            await asyncio.wait_for(self._task, 5)
        except Exception:
            pass
