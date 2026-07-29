"""A kernel manager that connects its sockets as event-loop-driven streams.

.. versionchanged:: 8.10
    ``connect_*`` can now return :class:`~jupyter_client.stream.AsyncZMQStream`,
    an asyncio-native stand-in for ``zmq.eventloop.zmqstream.ZMQStream``. Set
    :attr:`stream_class` to opt in; it becomes the default in jupyter-client 9.0,
    when tornado is dropped as a dependency.
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

Defaults to the tornado-based ``zmq.eventloop.zmqstream.ZMQStream``. Set this to
``jupyter_client.stream.AsyncZMQStream`` for an asyncio-native stream with the
same interface; that becomes the default in jupyter-client 9.0.
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
    """Convert a socket to a zmq stream."""

    def wrapped(self: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
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

    stream_class = Type(klass=object, help=_STREAM_CLASS_HELP, config=True)

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

    stream_class = Type(klass=object, help=_STREAM_CLASS_HELP, config=True)

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
