"""A basic in process kernel monitor with autorestarting.

This watches a kernel's state using KernelManager.is_alive and auto
restarts the kernel if it dies.
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import asyncio
import inspect
import time
import warnings
from typing import Any

from traitlets import Any as AnyTrait
from traitlets import default

from ..restarter import KernelRestarter
from ..stream import ensure_event_loop


class IOLoopKernelRestarter(KernelRestarter):
    """Monitor and autorestart a kernel.

    .. versionchanged:: 8.10
        Polling is driven by :mod:`asyncio` instead of
        ``tornado.ioloop.PeriodicCallback``.
    """

    # Deprecated. Declared as Any rather than Instance("tornado.ioloop.IOLoop"):
    # traitlets resolves Instance class strings at instance-init time, which
    # would import tornado for every restarter ever constructed.
    loop = AnyTrait()

    @default("loop")
    def _loop_default(self) -> Any:
        warnings.warn(
            "IOLoopKernelRestarter.loop is deprecated in jupyter-client 5.2"
            " and will be removed in 9.0. Polling now runs on the asyncio"
            " event loop; use asyncio.get_running_loop() instead.",
            DeprecationWarning,
            stacklevel=4,
        )
        from tornado import ioloop

        return ioloop.IOLoop.current()

    #: Deprecated, and now always ``None``: polling no longer goes through a
    #: tornado PeriodicCallback. Kept so that ``self._pcallback is None`` checks
    #: in subclasses do not raise AttributeError.
    _pcallback = None

    _timer_handle: asyncio.TimerHandle | None = None
    _poll_task: asyncio.Task | None = None
    _stopped = True

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """The asyncio loop to schedule polls on."""
        # If a caller explicitly supplied a tornado IOLoop, honor it.
        if "loop" in self._trait_values:
            asyncio_loop = getattr(self._trait_values["loop"], "asyncio_loop", None)
            if asyncio_loop is not None:
                return asyncio_loop
        return ensure_event_loop()

    def start(self) -> None:
        """Start the polling of the kernel."""
        if not self._stopped:
            return
        self._stopped = False
        self._schedule_next()

    def stop(self) -> None:
        """Stop the kernel polling."""
        self._stopped = True
        if self._timer_handle is not None:
            self._timer_handle.cancel()
            self._timer_handle = None
        if self._poll_task is not None:
            # poll() may call stop() on itself when the restart limit is hit;
            # cancelling the task we are running inside of would be unhelpful.
            try:
                current = asyncio.current_task()
            except RuntimeError:  # pragma: no cover - no running loop
                current = None
            if self._poll_task is not current:
                self._poll_task.cancel()
            self._poll_task = None

    def _schedule_next(self) -> None:
        """Arm the timer for the next poll."""
        if self._stopped:
            return
        self._timer_handle = self._get_loop().call_later(self.time_to_dead, self._run_poll)

    def _run_poll(self) -> None:
        """Timer callback: poll once, then re-arm."""
        self._timer_handle = None
        if self._stopped:
            return
        result: Any = None
        try:
            # KernelRestarter.poll is sync; AsyncIOLoopKernelRestarter.poll is a
            # coroutine function. Both shapes are handled below.
            result = self.poll()  # type:ignore[func-returns-value]
        except Exception:
            self.log.exception("KernelRestarter: poll failed")

        if inspect.isawaitable(result):
            # AsyncIOLoopKernelRestarter.poll is a coroutine; re-arm only once
            # it has finished so that polls cannot overlap. Wrap the coroutine
            # directly rather than nesting it inside another one: cancelling the
            # task must close the coroutine, not leave it un-awaited.
            task = asyncio.ensure_future(result)
            self._poll_task = task
            task.add_done_callback(self._poll_done)
        else:
            self._schedule_next()

    def _poll_done(self, task: asyncio.Future) -> None:
        """Re-arm the timer once an async poll has settled."""
        self._poll_task = None
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.log.error("KernelRestarter: poll failed", exc_info=exc)
        self._schedule_next()


class AsyncIOLoopKernelRestarter(IOLoopKernelRestarter):
    """An async io loop kernel restarter."""

    async def poll(self) -> None:  # type:ignore[override]
        """Poll the kernel."""
        if self.debug:
            self.log.debug("Polling kernel...")
        is_alive = await self.kernel_manager.is_alive()
        now = time.time()
        if not is_alive:
            self._last_dead = now
            if self._restarting:
                self._restart_count += 1
            else:
                self._restart_count = 1

            if self._restart_count > self.restart_limit:
                self.log.warning("AsyncIOLoopKernelRestarter: restart failed")
                self._fire_callbacks("dead")
                self._restarting = False
                self._restart_count = 0
                self.stop()
            else:
                newports = self.random_ports_until_alive and self._initial_startup
                self.log.info(
                    "AsyncIOLoopKernelRestarter: restarting kernel (%i/%i), %s random ports",
                    self._restart_count,
                    self.restart_limit,
                    "new" if newports else "keep",
                )
                self._fire_callbacks("restart")
                await self.kernel_manager.restart_kernel(now=True, newports=newports)
                self._restarting = True
        else:
            # Since `is_alive` only tests that the kernel process is alive, it does not
            # indicate that the kernel has successfully completed startup. To solve this
            # correctly, we would need to wait for a kernel info reply, but it is not
            # necessarily appropriate to start a kernel client + channels in the
            # restarter. Therefore, we use "has been alive continuously for X time" as a
            # heuristic for a stable start up.
            # See https://github.com/jupyter/jupyter_client/pull/717 for details.
            stable_start_time = self.stable_start_time
            if self.kernel_manager.provisioner:
                stable_start_time = self.kernel_manager.provisioner.get_stable_start_time(
                    recommended=stable_start_time
                )
            if self._initial_startup and now - self._last_dead >= stable_start_time:
                self._initial_startup = False
            if self._restarting and now - self._last_dead >= stable_start_time:
                self.log.debug("AsyncIOLoopKernelRestarter: restart apparently succeeded")
                self._restarting = False
