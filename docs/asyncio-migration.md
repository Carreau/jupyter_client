# Moving off tornado

Historically jupyter-client drove its ZMQ sockets with `tornado.ioloop.IOLoop`,
by way of `zmq.eventloop.zmqstream.ZMQStream`. Since tornado 6 the `IOLoop` has
been a thin wrapper over `asyncio`, so that indirection buys nothing. This page
describes where jupyter-client is going and how to get there.

## Where this is going

**Await the socket.** `AsyncKernelManager.connect_shell()` already returns a
`zmq.asyncio.Socket`, and pyzmq already knows how to drive it:

```python
km = AsyncIOLoopKernelManager()
km.stream_class = None  # the default in 9.0

await km.start_kernel()
shell = km.connect_shell()

km.session.send(shell, "kernel_info_request")
idents, msg = km.session.feed_identities(await shell.recv_multipart())
reply = km.session.deserialize(msg)
```

To consume a channel continuously, own a task rather than registering a
callback:

```python
async with asyncio.TaskGroup() as tg:
    for name in ("shell", "iopub", "control"):
        tg.create_task(pump(name, channels[name]))


async def pump(name, socket):
    while True:
        msg_list = await socket.recv_multipart()
        await handle(name, msg_list)
```

This is not merely more idiomatic. It fixes three things the callback interface
cannot express:

- **Exceptions have somewhere to go.** When an `on_recv` callback raises, there
  is no caller to propagate to, so the stream can only log and swallow it. An
  exception raised in a task surfaces where the work was requested, and a
  `TaskGroup` cancels its siblings.

- **Backpressure becomes expressible** — on the channels where it is safe. A
  callback stream reads the socket as fast as messages arrive and pushes them at
  the consumer, which has no way to say "stop". In the pull model, not calling
  `recv_multipart()` leaves the messages in ZMQ's buffer. Note the
  `await handle(...)` above: in a synchronous callback you cannot await the
  downstream write at all.

  **Do not do this on iopub.** iopub is an XPUB/SUB channel, and libzmq
  *silently discards* messages once a PUB socket reaches its high-water mark
  (1000 by default; neither ipykernel nor jupyter-server raises it). Declining
  to read iopub loses kernel output with no error anywhere — the failure mode
  behind [nbconvert#1183](https://github.com/jupyter/nbconvert/pull/1183),
  whose fix was to drain iopub *more* eagerly. Apply backpressure to the
  request/reply channels, where the backlog is bounded by outstanding requests,
  and keep draining iopub promptly. The Jupyter messaging spec says nothing
  normative about flow control, so this is a property of the transport rather
  than of the protocol.

- **Cancellation is ordinary.** `task.cancel()` replaces `stop_on_recv()`, and
  task lifetime replaces hand-rolled teardown bookkeeping.

## The adapter

{class}`~jupyter_client.stream.AsyncZMQStream` implements the subset of the
`ZMQStream` API that Jupyter uses -- `on_recv`, `on_recv_stream`,
`stop_on_recv`, `send`, `send_multipart`, `flush`, `close`, `closed` -- with no
tornado involved.

It exists so that code built on `on_recv` keeps working while it migrates. It is
**not** the destination: it inherits every limitation listed above, because
those are properties of the callback shape rather than of the implementation.
Roughly two thirds of it re-implements what `zmq.asyncio` already does. Prefer
`stream_class = None` for anything new, and treat `AsyncZMQStream` as a
transitional step for existing consumers.

| `stream_class` | `connect_*` returns | Status |
| --- | --- | --- |
| `None` | `zmq.asyncio.Socket` (async manager) | Recommended; default in 9.0 |
| `AsyncZMQStream` | tornado-free `on_recv` adapter | Transitional |
| `ZMQStream` | tornado stream | Current default; deprecated |

## What else changed in 8.10

All of this is backwards compatible.

**The kernel restarter no longer uses `tornado.ioloop.PeriodicCallback`.** It
schedules itself with `asyncio`. `start()` and `stop()` are unchanged.

**`KernelApp` runs on a bare asyncio loop.** `KernelApp.loop` is an
`asyncio.AbstractEventLoop`. Signal handling moved from
`IOLoop.add_callback_from_signal` (deprecated in tornado 6) to
`loop.add_signal_handler`.

**`ThreadedKernelClient` runs a plain asyncio loop in its background thread.**
`ThreadedKernelClient.ioloop` and `IOLoopThread.ioloop` are asyncio loops, and
`ThreadedZMQSocketChannel.stream` is an `AsyncZMQStream`. Replace
`ioloop.add_callback(fn)` with `ioloop.call_soon_threadsafe(fn)`. Stopping the
thread is now immediate rather than taking up to a second.

## Deprecations

Deprecated in 8.10, removed in 9.0. Each emits a `DeprecationWarning` on first
access.

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
read the trait. The traits still return a tornado `IOLoop` when read. The only
loss is type validation on a trait that is going away.

## Planned for 9.0

- `stream_class` defaults to `None`.
- The deprecated `loop` traits are removed.
- `tornado` is dropped from `dependencies`.
- `AsyncZMQStream` remains, as an adapter, for consumers still on `on_recv`.

Until tornado is dropped it remains an indirect install dependency anyway,
because `zmq.eventloop.zmqstream` imports it.

## A note on Windows

`AsyncZMQStream` uses `loop.add_reader`, which asyncio's IOCP-based
`ProactorEventLoop` -- the default on Windows -- does not implement, so a
selector event loop is required. Jupyter applications already set
`WindowsSelectorEventLoopPolicy` on startup, and tornado's `IOLoop` imposed the
same constraint, so this is not new.

`zmq.asyncio` handles this case better than the adapter does: it falls back to a
background selector thread on Proactor loops. That fallback uses tornado's
`AddThreadSelectorEventLoop` when tornado happens to be installed, which is one
more reason to treat "jupyter-client does not require tornado" as separate from
"tornado is never present".
