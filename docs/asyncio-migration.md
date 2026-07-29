# Moving off tornado

Historically jupyter-client drove its ZMQ sockets with `tornado.ioloop.IOLoop`,
mostly by way of `zmq.eventloop.zmqstream.ZMQStream`. Since tornado 6 the
`IOLoop` has been a thin wrapper over `asyncio`, so almost none of that
indirection buys anything any more. jupyter-client is removing it.

## What changed in 8.10

Everything below is backwards compatible: the same classes and methods work,
and tornado is still installed.

**`jupyter_client.stream.AsyncZMQStream` is new.** It implements the subset of
the `ZMQStream` API that Jupyter actually uses — `on_recv`, `on_recv_stream`,
`stop_on_recv`, `send`, `send_multipart`, `flush`, `close`, `closed` — directly
against `asyncio`, with no tornado involved. Like `ZMQStream`, it drives a
*synchronous* `zmq.Socket` by watching the socket's `ZMQ_FD`.

**`IOLoopKernelManager` and `AsyncIOLoopKernelManager` gained a `stream_class`
trait.** It selects what `connect_shell()`, `connect_iopub()`, and friends
return. In 8.x it defaults to tornado's `ZMQStream`; in 9.0 it will default to
`AsyncZMQStream`. To opt in early:

```python
from jupyter_client.ioloop import AsyncIOLoopKernelManager
from jupyter_client.stream import AsyncZMQStream

km = AsyncIOLoopKernelManager()
km.stream_class = AsyncZMQStream
```

or via config:

```python
c.AsyncIOLoopKernelManager.stream_class = "jupyter_client.stream.AsyncZMQStream"
```

**The kernel restarter no longer uses `tornado.ioloop.PeriodicCallback`.** It
schedules itself with `asyncio` instead. `IOLoopKernelRestarter.start()` and
`.stop()` are unchanged.

**`KernelApp` runs on a bare asyncio loop.** `KernelApp.loop` is now an
`asyncio.AbstractEventLoop`. Signal handling moved from
`IOLoop.add_callback_from_signal` (deprecated in tornado 6) to
`loop.add_signal_handler`.

**`ThreadedKernelClient` runs a plain asyncio loop in its background thread.**
`ThreadedKernelClient.ioloop` and `IOLoopThread.ioloop` are now
`asyncio.AbstractEventLoop` objects, and `ThreadedZMQSocketChannel.stream` is an
`AsyncZMQStream`. If you scheduled work on that loop yourself, replace
`ioloop.add_callback(fn)` with `ioloop.call_soon_threadsafe(fn)`.

## Deprecations

The following are deprecated in 8.10 and removed in 9.0. Each emits a
`DeprecationWarning` on first access.

| Deprecated | Replacement |
| --- | --- |
| `SessionFactory.loop` | `asyncio.get_running_loop()` |
| `IOLoopKernelManager.loop` | `asyncio.get_running_loop()` |
| `AsyncIOLoopKernelManager.loop` | `asyncio.get_running_loop()` |
| `IOLoopKernelRestarter.loop` | `asyncio.get_running_loop()` |

`IOLoopKernelRestarter._pcallback` is retained as `None`; there is no longer a
`PeriodicCallback` behind it.

Each of those `loop` traits was declared as `Instance("tornado.ioloop.IOLoop")`
and is now declared as `Any`. This is deliberate: traitlets resolves an
`Instance` class string during *instance init*, so merely constructing a
`Session` or a kernel manager imported tornado, whether or not anything ever
read the trait. The traits still return a tornado `IOLoop` when read; they just
no longer drag tornado into every object's construction. The only loss is trait
type validation on a trait that is going away.

## What is still to come, in 9.0

- `stream_class` defaults to `AsyncZMQStream`.
- The deprecated `loop` traits are removed.
- `tornado` is dropped from `dependencies`.

Note that until that last step lands, tornado remains an indirect install
dependency anyway, because `zmq.eventloop.zmqstream` imports it.

## A note on Windows

`AsyncZMQStream` uses `loop.add_reader`, which asyncio's IOCP-based
`ProactorEventLoop` — the default on Windows — does not implement. A selector
event loop is required there. This is not a new constraint: tornado's `IOLoop`
has the same requirement, and Jupyter applications already set
`WindowsSelectorEventLoopPolicy` on startup.
