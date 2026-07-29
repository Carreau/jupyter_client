"""End-to-end tests for driving a real kernel through AsyncZMQStream.

These mirror what jupyter-server does with the streams returned by the
``connect_*`` methods: register an ``on_recv`` callback, send a request through
the Session, and expect the reply to arrive on the event loop.
"""

import asyncio

import pytest
from traitlets.config.loader import Config

from jupyter_client.ioloop import AsyncIOLoopKernelManager, IOLoopKernelManager
from jupyter_client.stream import AsyncZMQStream


def _config(stream_class):
    c = Config()
    c.IOLoopKernelManager.stream_class = stream_class
    c.AsyncIOLoopKernelManager.stream_class = stream_class
    return c


async def _await_reply(stream, session, timeout=20.0):
    """Wait for one deserialized message to arrive on a stream."""
    received: asyncio.Future = asyncio.get_running_loop().create_future()

    def on_recv(msg_list):
        if received.done():
            return
        _idents, fed = session.feed_identities(msg_list)
        received.set_result(session.deserialize(fed, content=False))

    stream.on_recv(on_recv)
    return await asyncio.wait_for(received, timeout)


@pytest.mark.timeout(60)
async def test_async_manager_round_trip_with_async_stream():
    """A kernel_info_request/reply round trip over AsyncZMQStream."""
    km = AsyncIOLoopKernelManager(config=_config(AsyncZMQStream))
    assert km.stream_class is AsyncZMQStream

    await km.start_kernel(stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    shell = km.connect_shell()
    assert isinstance(shell, AsyncZMQStream)
    try:
        km.session.send(shell, "kernel_info_request")
        reply = await _await_reply(shell, km.session)
        assert reply["header"]["msg_type"] == "kernel_info_reply"
    finally:
        shell.close()
        await km.shutdown_kernel(now=True)


@pytest.mark.timeout(60)
async def test_async_manager_iopub_activity_with_async_stream():
    """iopub traffic reaches an AsyncZMQStream callback, as jupyter-server relies on."""
    km = AsyncIOLoopKernelManager(config=_config(AsyncZMQStream))
    await km.start_kernel(stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    iopub = km.connect_iopub()
    shell = km.connect_shell()
    received: list = []
    iopub.on_recv(received.append)
    try:
        # A SUB socket may still be completing its subscription when the first
        # request goes out (zmq's "slow joiner"), so keep asking until something
        # lands rather than relying on a single request being observed.
        deadline = asyncio.get_running_loop().time() + 30
        while not received and asyncio.get_running_loop().time() < deadline:
            km.session.send(shell, "kernel_info_request")
            await asyncio.sleep(0.2)
        assert received, "no iopub traffic reached the stream callback"
        assert not iopub.closed()
    finally:
        iopub.close()
        shell.close()
        await km.shutdown_kernel(now=True)


@pytest.mark.timeout(60)
async def test_stream_close_releases_socket():
    km = AsyncIOLoopKernelManager(config=_config(AsyncZMQStream))
    await km.start_kernel(stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stream = km.connect_shell()
    socket = stream.socket
    try:
        assert not stream.closed()
        stream.close()
        assert stream.closed()
        assert socket.closed
        # closing twice is a no-op, as with ZMQStream
        stream.close()
    finally:
        await km.shutdown_kernel(now=True)


@pytest.mark.timeout(60)
def test_sync_manager_round_trip_with_async_stream():
    """The synchronous manager binds streams to the loop run_sync drives."""
    km = IOLoopKernelManager(config=_config(AsyncZMQStream))
    km.start_kernel(stdout=None, stderr=None)
    shell = km.connect_shell()
    assert isinstance(shell, AsyncZMQStream)

    received = []
    shell.on_recv(received.append)
    try:
        km.session.send(shell, "kernel_info_request")
        # The stream is bound to the loop the kernel was started on; running
        # anything on that loop lets the reply be dispatched.
        loop = shell.io_loop
        deadline = loop.time() + 20

        async def pump():
            while not received and loop.time() < deadline:
                await asyncio.sleep(0.05)

        loop.run_until_complete(pump())
        assert received, "no reply dispatched to the stream callback"
    finally:
        shell.close()
        km.shutdown_kernel(now=True)


@pytest.mark.timeout(60)
def test_default_stream_class_is_zmqstream_when_tornado_present():
    """8.x keeps the tornado stream as the default for backwards compatibility."""
    pytest.importorskip("tornado")
    from zmq.eventloop.zmqstream import ZMQStream

    assert IOLoopKernelManager().stream_class is ZMQStream
    assert AsyncIOLoopKernelManager().stream_class is ZMQStream
