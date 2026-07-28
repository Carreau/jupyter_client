"""Tests for the asyncio-native AsyncZMQStream."""

import asyncio

import pytest
import zmq

from jupyter_client.stream import AsyncZMQStream


@pytest.fixture()
def context():
    ctx = zmq.Context()
    yield ctx
    ctx.destroy(linger=0)


@pytest.fixture()
def pair(context):
    """A connected PAIR of sync sockets."""
    a = context.socket(zmq.PAIR)
    b = context.socket(zmq.PAIR)
    port = a.bind_to_random_port("tcp://127.0.0.1")
    b.connect(f"tcp://127.0.0.1:{port}")
    yield a, b
    a.close(linger=0)
    b.close(linger=0)


async def _wait_for(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0.01)


async def test_on_recv(pair):
    a, b = pair
    stream = AsyncZMQStream(a)
    received = []
    stream.on_recv(received.append)

    b.send_multipart([b"hello", b"world"])
    await _wait_for(lambda: received)
    assert received == [[b"hello", b"world"]]
    stream.close()


async def test_on_recv_stream(pair):
    a, b = pair
    stream = AsyncZMQStream(a)
    received = []
    stream.on_recv_stream(lambda s, msg: received.append((s, msg)))

    b.send_multipart([b"x"])
    await _wait_for(lambda: received)
    assert received[0][0] is stream
    assert received[0][1] == [b"x"]
    stream.close()


async def test_recv_registered_after_message_arrives(pair):
    """ZMQ_FD is edge triggered: a message pending before on_recv must still fire."""
    a, b = pair
    stream = AsyncZMQStream(a)
    b.send_multipart([b"early"])
    # give the message time to actually land on the socket
    await asyncio.sleep(0.2)

    received = []
    stream.on_recv(received.append)
    await _wait_for(lambda: received)
    assert received == [[b"early"]]
    stream.close()


async def test_stop_on_recv(pair):
    a, b = pair
    stream = AsyncZMQStream(a)
    received = []
    stream.on_recv(received.append)
    b.send_multipart([b"one"])
    await _wait_for(lambda: received)

    stream.stop_on_recv()
    assert not stream.receiving()
    b.send_multipart([b"two"])
    await asyncio.sleep(0.3)
    assert received == [[b"one"]]

    # re-registering picks up the buffered message
    stream.on_recv(received.append)
    await _wait_for(lambda: len(received) == 2)
    assert received[1] == [b"two"]
    stream.close()


async def test_on_recv_none_stops(pair):
    a, b = pair
    stream = AsyncZMQStream(a)
    received = []
    stream.on_recv(received.append)
    stream.on_recv(None)
    b.send_multipart([b"nope"])
    await asyncio.sleep(0.3)
    assert received == []
    stream.close()


async def test_send(pair):
    a, b = pair
    stream = AsyncZMQStream(a)
    stream.send_multipart([b"a", b"b"])
    assert b.poll(timeout=5000)
    assert b.recv_multipart() == [b"a", b"b"]
    stream.close()


async def test_send_ordering_under_backpressure(context):
    """Messages queued because of a full send buffer keep their order."""
    push = context.socket(zmq.PUSH)
    push.setsockopt(zmq.SNDHWM, 1)
    push.setsockopt(zmq.LINGER, 0)
    port = push.bind_to_random_port("tcp://127.0.0.1")

    stream = AsyncZMQStream(push)
    total = 200
    for i in range(total):
        stream.send_multipart([b"%d" % i])

    # With no peer connected, a PUSH socket cannot send at all, so everything
    # above went onto the stream's queue.
    assert stream.sending()

    pull = context.socket(zmq.PULL)
    pull.setsockopt(zmq.RCVHWM, 1)
    pull.connect(f"tcp://127.0.0.1:{port}")

    got = []
    deadline = asyncio.get_running_loop().time() + 20
    while len(got) < total:
        if asyncio.get_running_loop().time() > deadline:
            break
        if pull.poll(timeout=0):
            got.append(int(pull.recv_multipart()[0]))
        else:
            # yield so the stream's writability callback can flush its queue
            await asyncio.sleep(0.001)

    assert got == list(range(total))
    assert not stream.sending()
    stream.close()
    pull.close(linger=0)


async def test_flush(pair):
    a, b = pair
    stream = AsyncZMQStream(a)
    received = []
    stream.on_recv(received.append)
    b.send_multipart([b"1"])
    b.send_multipart([b"2"])
    # wait for the messages to be readable without letting the loop dispatch
    assert a.poll(timeout=5000)
    handled = stream.flush()
    assert handled >= 1
    assert received
    stream.close()


async def test_close_is_idempotent(pair):
    a, _b = pair
    stream = AsyncZMQStream(a)
    stream.on_recv(lambda msg: None)
    stream.close()
    assert stream.closed()
    stream.close()
    with pytest.raises(OSError):
        stream.send_multipart([b"x"])


async def test_rejects_async_socket():
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.PAIR)
    try:
        with pytest.raises(TypeError):
            AsyncZMQStream(sock)
    finally:
        sock.close(linger=0)
        ctx.destroy(linger=0)


async def test_many_messages_do_not_starve_the_loop(pair):
    """Draining a firehose must yield back to the event loop."""
    a, b = pair
    stream = AsyncZMQStream(a)
    received = []
    stream.on_recv(received.append)

    count = 500
    for i in range(count):
        b.send_multipart([b"%d" % i])

    ticks = 0

    async def ticker():
        nonlocal ticks
        while len(received) < count:
            ticks += 1
            await asyncio.sleep(0)

    await asyncio.wait_for(ticker(), timeout=10)
    assert len(received) == count
    assert [int(m[0]) for m in received] == list(range(count))
    # the loop got control back while draining
    assert ticks > 1
    stream.close()
