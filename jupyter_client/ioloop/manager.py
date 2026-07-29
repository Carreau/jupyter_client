"""A kernel manager whose connect_* methods can wrap their sockets.

.. versionchanged:: 8.10
    :attr:`stream_class` decides what ``connect_*`` returns. Setting it to
    ``None`` returns the socket unwrapped -- a ``zmq.asyncio.Socket`` for
    :class:`AsyncIOLoopKernelManager` -- which you drive by awaiting it. That is
    the recommended target and becomes the default in jupyter-client 9.0.

    The callback-style wrappers remain for code that has not moved yet:
    :class:`~jupyter_client.stream.AsyncZMQStream` is a tornado-free adapter with
    the old ``on_recv`` interface, and tornado's ``ZMQStream`` is deprecated.
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import asyncio
import typing as t
import warnings

import zmq
from traitlets import Any as AnyTrait
from traitlets import Instance, Type, default

from ..manager import AsyncKernelManager, KernelManager
from ..stream import AsyncZMQStream, ensure_event_loop
from .restarter import AsyncIOLoopKernelRestarter, IOLoopKernelRestarter

_STREAM_CLASS_HELP = """\
Class used to wrap the sockets returned by the connect_* methods.

Set this to ``None`` -- the recommended choice, and the default from
jupyter-client 9.0 -- to get the underlying socket with no wrapper at all. For
AsyncKernelManager that is a ``zmq.asyncio.Socket``, which you drive by awaiting
it directly::

    msg = await kernel_manager.connect_shell().recv_multipart()

Two callback-style wrappers remain available for code that has not moved to
awaiting sockets yet. ``jupyter_client.stream.AsyncZMQStream`` is an adapter
offering the old ``on_recv`` interface without tornado, and the tornado-based
``zmq.eventloop.zmqstream.ZMQStream`` (the current default) is deprecated.
"""

_LOOP_DEPRECATION = (
    "{name}.loop is deprecated in jupyter-client 8.10 and will be removed in 9.0."
    " Streams and the restarter now run on the asyncio event loop;"
    " use asyncio.get_running_loop() instead."
)


def _default_stream_class() -> t.Any:
    """ZMQStream when tornado is available, else the asyncio-native stream."""
    try:
        from zmq.eventloop.zmqstream import ZMQStream
    except ImportError:  # pragma: no cover - tornado not installed
        return AsyncZMQStream
    return ZMQStream


def as_zmqstream(f: t.Any) -> t.Callable:
    """Convert a socket to a zmq stream, unless ``stream_class`` is None."""

    def wrapped(self: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
        if self.stream_class is None:
            # No wrapper: hand back the socket the base manager built. For
            # AsyncKernelManager that is a zmq.asyncio.Socket, which the caller
            # awaits directly -- no callbacks, and zmq applies backpressure
            # because nothing is read until the caller asks for it.
            return f(self, *args, **kwargs)

        save_socket_class = None
        # zmqstreams only support sync sockets
        if self.context._socket_class is not zmq.Socket:
            save_socket_class = self.context._socket_class
            self.context._socket_class = zmq.Socket
        try:
            socket = f(self, *args, **kwargs)
        finally:
            if save_socket_class:
                # restore default socket class
                self.context._socket_class = save_socket_class
        return self._make_stream(socket)

    return wrapped


def _tornado_ioloop(loop: asyncio.AbstractEventLoop) -> t.Any:
    """The tornado IOLoop wrapping an asyncio loop.

    ``IOLoop.current()`` is the only way to get at tornado's per-asyncio-loop
    wrapper, and it reads the *current* loop -- so make ``loop`` current for the
    duration of the call when it is not already.
    """
    from tornado.ioloop import IOLoop

    try:
        if asyncio.get_running_loop() is loop:
            return IOLoop.current()
    except RuntimeError:
        pass

    try:
        previous: asyncio.AbstractEventLoop | None = asyncio.get_event_loop()
    except RuntimeError:
        previous = None
    asyncio.set_event_loop(loop)
    try:
        return IOLoop.current()
    finally:
        asyncio.set_event_loop(previous)


def _make_stream(self: t.Any, socket: zmq.Socket) -> t.Any:
    """Wrap a socket in ``self.stream_class``."""
    if "loop" in self._trait_values:
        # A loop was explicitly supplied; keep passing it through.
        return self.stream_class(socket, self._trait_values["loop"])

    loop = self._stream_loop or ensure_event_loop()
    if isinstance(self.stream_class, type) and issubclass(self.stream_class, AsyncZMQStream):
        return self.stream_class(socket, loop)
    # tornado's ZMQStream wants an IOLoop rather than a bare asyncio loop.
    return self.stream_class(socket, _tornado_ioloop(loop))


def _record_stream_loop(self: t.Any) -> None:
    """Remember the event loop the kernel was started on.

    Streams must be driven by a loop that actually runs. For a synchronous
    KernelManager the caller is typically never inside a coroutine when it calls
    ``connect_*``, so resolving the loop at connect time would bind the stream to
    an idle loop and its callbacks would never fire. Kernel startup, on the other
    hand, always happens on a live loop (directly, or via ``run_sync``), so that
    is the loop to bind to. This reproduces the binding that fell out of the
    old ``self.loop`` trait being first resolved during startup.
    """
    self._stream_loop = ensure_event_loop()


class IOLoopKernelManager(KernelManager):
    """An io loop kernel manager."""

    stream_class = Type(klass=object, allow_none=True, help=_STREAM_CLASS_HELP, config=True)

    @default("stream_class")
    def _stream_class_default(self) -> t.Any:
        return _default_stream_class()

    # Deprecated. Declared as Any rather than Instance("tornado.ioloop.IOLoop"):
    # traitlets resolves Instance class strings at instance-init time, which
    # would import tornado for every kernel manager ever constructed.
    loop = AnyTrait()

    @default("loop")
    def _loop_default(self) -> t.Any:
        warnings.warn(
            _LOOP_DEPRECATION.format(name=type(self).__name__),
            DeprecationWarning,
            stacklevel=4,
        )
        from tornado import ioloop

        return ioloop.IOLoop.current()

    _make_stream = _make_stream
    _record_stream_loop = _record_stream_loop
    _stream_loop: t.Any = None

    restarter_class = Type(
        default_value=IOLoopKernelRestarter,
        klass=IOLoopKernelRestarter,
        help=(
            "Type of KernelRestarter to use. "
            "Must be a subclass of IOLoopKernelRestarter.\n"
            "Override this to customize how kernel restarts are managed."
        ),
        config=True,
    )
    _restarter: t.Any = Instance("jupyter_client.ioloop.IOLoopKernelRestarter", allow_none=True)

    def start_restarter(self) -> None:
        """Start the restarter."""
        self._record_stream_loop()
        if self.autorestart and self.has_kernel:
            if self._restarter is None:
                self._restarter = self.restarter_class(
                    kernel_manager=self, parent=self, log=self.log
                )
            self._restarter.start()

    def stop_restarter(self) -> None:
        """Stop the restarter."""
        if self.autorestart and self._restarter is not None:
            self._restarter.stop()

    connect_shell = as_zmqstream(KernelManager.connect_shell)
    connect_control = as_zmqstream(KernelManager.connect_control)
    connect_iopub = as_zmqstream(KernelManager.connect_iopub)
    connect_stdin = as_zmqstream(KernelManager.connect_stdin)
    connect_hb = as_zmqstream(KernelManager.connect_hb)


class AsyncIOLoopKernelManager(AsyncKernelManager):
    """An async ioloop kernel manager."""

    stream_class = Type(klass=object, allow_none=True, help=_STREAM_CLASS_HELP, config=True)

    @default("stream_class")
    def _stream_class_default(self) -> t.Any:
        return _default_stream_class()

    # Deprecated. Declared as Any rather than Instance("tornado.ioloop.IOLoop"):
    # traitlets resolves Instance class strings at instance-init time, which
    # would import tornado for every kernel manager ever constructed.
    loop = AnyTrait()

    @default("loop")
    def _loop_default(self) -> t.Any:
        warnings.warn(
            _LOOP_DEPRECATION.format(name=type(self).__name__),
            DeprecationWarning,
            stacklevel=4,
        )
        from tornado import ioloop

        return ioloop.IOLoop.current()

    _make_stream = _make_stream
    _record_stream_loop = _record_stream_loop
    _stream_loop: t.Any = None

    restarter_class = Type(
        default_value=AsyncIOLoopKernelRestarter,
        klass=AsyncIOLoopKernelRestarter,
        help=(
            "Type of KernelRestarter to use. "
            "Must be a subclass of AsyncIOLoopKernelManager.\n"
            "Override this to customize how kernel restarts are managed."
        ),
        config=True,
    )
    _restarter: t.Any = Instance(
        "jupyter_client.ioloop.AsyncIOLoopKernelRestarter", allow_none=True
    )

    def start_restarter(self) -> None:
        """Start the restarter."""
        self._record_stream_loop()
        if self.autorestart and self.has_kernel:
            if self._restarter is None:
                self._restarter = self.restarter_class(
                    kernel_manager=self, parent=self, log=self.log
                )
            self._restarter.start()

    def stop_restarter(self) -> None:
        """Stop the restarter."""
        if self.autorestart and self._restarter is not None:
            self._restarter.stop()

    connect_shell = as_zmqstream(AsyncKernelManager.connect_shell)
    connect_control = as_zmqstream(AsyncKernelManager.connect_control)
    connect_iopub = as_zmqstream(AsyncKernelManager.connect_iopub)
    connect_stdin = as_zmqstream(AsyncKernelManager.connect_stdin)
    connect_hb = as_zmqstream(AsyncKernelManager.connect_hb)
