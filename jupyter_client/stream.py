"""An asyncio-native replacement for :class:`zmq.eventloop.zmqstream.ZMQStream`.

``ZMQStream`` is implemented on top of ``tornado.ioloop.IOLoop``, which is the
last piece of machinery keeping tornado in jupyter-client's dependency tree.
:class:`AsyncZMQStream` provides the subset of the ``ZMQStream`` API that Jupyter
actually uses, implemented directly against :mod:`asyncio`.

Like ``ZMQStream``, this drives a *synchronous* :class:`zmq.Socket` by watching
the socket's ``ZMQ_FD`` for readiness and then draining ``ZMQ_EVENTS``. It is not
thread-safe: every method must be called from the thread running ``io_loop``.

.. note::

    ``asyncio`` event loops based on ``IOCP`` (the default on Windows) do not
    implement ``add_reader``. As with tornado, a selector event loop is required
    on Windows.
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import asyncio
import typing as t
from collections import deque

import zmq
import zmq.asyncio
from jupyter_core.utils import ensure_event_loop as jupyter_core_ensure_event_loop
from traitlets.log import get_logger

__all__ = ["AsyncZMQStream", "ensure_event_loop"]

# Maximum number of socket events handled in a single event loop callback.
# Draining an unbounded firehose (a chatty iopub channel, say) would otherwise
# starve every other callback on the loop.
MAX_EVENTS_PER_ITERATION = 100


def ensure_event_loop() -> asyncio.AbstractEventLoop:
    """Get the event loop that callbacks should be scheduled on.

    Mirrors what ``IOLoop.current()`` did for ``ZMQStream``: prefer the loop that
    is actually running, so that a stream created inside a coroutine is driven by
    that coroutine's loop.

    With no loop running, defer to :func:`jupyter_core.utils.ensure_event_loop`.
    That is the loop :func:`jupyter_core.utils.run_sync` will drive, and Jupyter's
    synchronous APIs are built on ``run_sync``: binding anywhere else would leave
    callbacks on a loop that never runs.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass
    return jupyter_core_ensure_event_loop()


class AsyncZMQStream:
    """Drive a zmq socket from an asyncio event loop.

    Parameters
    ----------
    socket : zmq.Socket
        A *synchronous* zmq socket. Passing a :class:`zmq.asyncio.Socket` is an
        error: this class does its own readiness handling.
    io_loop : asyncio.AbstractEventLoop, optional
        The loop to bind to. Defaults to the running loop. A
        ``tornado.ioloop.IOLoop`` is also accepted, for compatibility with
        callers that still hold one, and is unwrapped to its asyncio loop.
    """

    socket: zmq.Socket | None
    io_loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        socket: zmq.Socket,
        io_loop: t.Any = None,
    ) -> None:
        """Initialize the stream."""
        if isinstance(socket, zmq.asyncio.Socket):
            msg = (
                "AsyncZMQStream requires a synchronous zmq.Socket; "
                "a zmq.asyncio.Socket handles readiness itself and should be awaited directly."
            )
            raise TypeError(msg)

        self.socket = socket
        # Accept a tornado IOLoop so that callers holding one keep working.
        loop: t.Any = getattr(io_loop, "asyncio_loop", io_loop)
        self.io_loop = loop or ensure_event_loop()

        self._fd: int = int(socket.getsockopt(zmq.FD))
        self._recv_callback: t.Callable[..., t.Any] | None = None
        self._recv_copy = True
        self._pass_stream = False
        self._send_queue: deque[tuple[list[t.Any], int, bool, bool]] = deque()
        self._watching = False
        self._check_scheduled = False
        self._closed = False

    # -- introspection --------------------------------------------------

    def closed(self) -> bool:
        """Whether the stream (or its socket) has been closed."""
        if self._closed or self.socket is None:
            return True
        if self.socket.closed:
            # The socket was closed by somebody else; clean up after them, as
            # ZMQStream does, so we stop watching a dead file descriptor.
            self.close()
            return True
        return False

    def receiving(self) -> bool:
        """Whether the stream is currently receiving."""
        return self._recv_callback is not None

    def sending(self) -> bool:
        """Whether the stream has messages queued for sending."""
        return bool(self._send_queue)

    def __repr__(self) -> str:
        """A repr identifying the underlying socket."""
        return f"<{self.__class__.__name__}({self.socket!r})>"

    # -- receiving ------------------------------------------------------

    def on_recv(
        self,
        callback: t.Callable[[list[t.Any]], t.Any] | None,
        copy: bool = True,
    ) -> None:
        """Register a callback invoked with each incoming multipart message.

        Passing ``None`` stops receiving, as with ``ZMQStream``.
        """
        self._register_recv(callback, copy, pass_stream=False)

    def on_recv_stream(
        self,
        callback: t.Callable[[AsyncZMQStream, list[t.Any]], t.Any] | None,
        copy: bool = True,
    ) -> None:
        """Like :meth:`on_recv`, but the callback also receives this stream."""
        self._register_recv(callback, copy, pass_stream=True)

    def _register_recv(
        self,
        callback: t.Callable[..., t.Any] | None,
        copy: bool,
        pass_stream: bool,
    ) -> None:
        self._recv_callback = callback
        self._recv_copy = copy
        self._pass_stream = pass_stream and callback is not None
        self._update_watcher()

    def stop_on_recv(self) -> None:
        """Stop invoking the recv callback."""
        self._register_recv(None, self._recv_copy, pass_stream=False)

    # -- sending --------------------------------------------------------

    def send(
        self,
        msg: t.Any,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """Send a single-part message."""
        self.send_multipart([msg], flags=flags, copy=copy, track=track, **kwargs)

    def send_multipart(
        self,
        msg: list[t.Any],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """Send a multipart message.

        Sends immediately when the socket can accept the message, which is the
        common case; otherwise the message is queued and flushed once the socket
        becomes writable. Queued messages preserve ordering.
        """
        if self.closed():
            msg_ = "Cannot send on a closed stream."
            raise OSError(msg_)

        if self._send_queue:
            # Something is already queued; appending keeps ordering intact.
            self._send_queue.append((list(msg), flags, copy, track))
            self._update_watcher()
            return

        try:
            self._send_now(list(msg), flags, copy, track)
        except zmq.Again:
            self._send_queue.append((list(msg), flags, copy, track))
            self._update_watcher()

    def _send_now(self, msg: list[t.Any], flags: int, copy: bool, track: bool) -> None:
        assert self.socket is not None
        self.socket.send_multipart(msg, flags | zmq.NOBLOCK, copy=copy, track=track)

    # -- lifecycle ------------------------------------------------------

    def flush(
        self,
        flag: int = zmq.POLLIN | zmq.POLLOUT,
        limit: int | None = None,
    ) -> int:
        """Synchronously process pending events on the socket.

        Returns the number of events handled. Unlike :meth:`_handle_events` this
        does not yield back to the event loop, so it can be used to guarantee
        that all currently-pending messages have been dispatched.
        """
        return self._process_events(flag=flag, limit=limit, reschedule=False)

    def close(self, linger: int | None = None) -> None:
        """Close the stream and its socket."""
        if self._closed:
            return
        self._closed = True
        self._recv_callback = None
        self._send_queue.clear()
        self._stop_watching()
        socket, self.socket = self.socket, None
        if socket is not None and not socket.closed:
            socket.close(linger=linger)

    # -- event handling -------------------------------------------------

    def _update_watcher(self) -> None:
        """Add or remove the fd watcher to match what the stream wants to do."""
        if self.closed():
            self._stop_watching()
            return

        wants_events = self._recv_callback is not None or bool(self._send_queue)
        if wants_events and not self._watching:
            self.io_loop.add_reader(self._fd, self._handle_events)
            self._watching = True
            # The socket may already have events pending, and ZMQ_FD is
            # edge-triggered: without an explicit check we could wait forever
            # for an edge that has already passed.
            self._schedule_check()
        elif not wants_events and self._watching:
            self._stop_watching()

    def _stop_watching(self) -> None:
        if self._watching:
            try:
                self.io_loop.remove_reader(self._fd)
            except (OSError, ValueError, RuntimeError):  # pragma: no cover
                # Loop already closed, or fd no longer valid.
                pass
            self._watching = False

    def _schedule_check(self) -> None:
        if self._check_scheduled:
            return
        self._check_scheduled = True

        def _check() -> None:
            self._check_scheduled = False
            self._handle_events()

        self.io_loop.call_soon(_check)

    def _handle_events(self) -> None:
        """Callback for fd readiness: drain whatever the socket has to offer."""
        if self.closed():
            self._stop_watching()
            return
        self._process_events(reschedule=True)

    def _process_events(
        self,
        flag: int = zmq.POLLIN | zmq.POLLOUT,
        limit: int | None = None,
        reschedule: bool = True,
    ) -> int:
        """Drain socket events, dispatching recv callbacks and queued sends."""
        if limit is None and reschedule:
            # Bound event-loop callbacks; flush() (reschedule=False) is unbounded,
            # matching ZMQStream, because its caller wants everything drained now.
            limit = MAX_EVENTS_PER_ITERATION
        count = 0
        while True:
            if self.closed():
                break
            try:
                events = int(self.socket.getsockopt(zmq.EVENTS))  # type:ignore[union-attr]
            except zmq.ZMQError:
                # Socket closed or otherwise unusable underneath us.
                self.close()
                break

            if flag & zmq.POLLOUT and events & zmq.POLLOUT and self._send_queue:
                # Drain sends before recvs: a full send queue means we are
                # applying backpressure to somebody.
                self._handle_send()
            elif flag & zmq.POLLIN and events & zmq.POLLIN and self._recv_callback is not None:
                self._handle_recv()
            else:
                break

            count += 1
            if limit is not None and count >= limit:
                # Yield to the event loop rather than starving other callbacks,
                # then pick up where we left off.
                self._schedule_check()
                return count

        if reschedule:
            self._update_watcher()
        return count

    def _handle_recv(self) -> None:
        assert self.socket is not None
        try:
            msg_list = self.socket.recv_multipart(zmq.NOBLOCK, copy=self._recv_copy)
        except zmq.Again:
            return
        except zmq.ZMQError as e:
            get_logger().error("Error receiving on %r: %s", self, e)
            return

        callback = self._recv_callback
        if callback is None:
            return
        try:
            if self._pass_stream:
                callback(self, msg_list)
            else:
                callback(msg_list)
        except Exception:
            get_logger().exception("Uncaught exception in recv callback on %r", self)

    def _handle_send(self) -> None:
        msg, flags, copy, track = self._send_queue[0]
        try:
            self._send_now(msg, flags, copy, track)
        except zmq.Again:
            return
        except zmq.ZMQError as e:
            self._send_queue.popleft()
            get_logger().error("Error sending on %r: %s", self, e)
            return
        self._send_queue.popleft()
