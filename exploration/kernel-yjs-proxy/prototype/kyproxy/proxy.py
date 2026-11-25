"""The kernel proxy: a Y collaborator on the notebook doc + side kernel doc,
and a jupyter_client client of a real kernel.

Threading model: everything runs on one asyncio loop. Y observers never write
to docs; they only set `self._wake`. All writes happen in proxy tasks inside
`transaction(origin=PROXY_ORIGIN)`, and observers ignore that origin.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict

from jupyter_client import AsyncKernelManager
from jupyter_ydoc import YNotebook
from pycrdt import Array, Map, Text

from .schema import PROXY_ORIGIN, KernelDoc, process_text, sort_key
from .transport import DocConnection

log = logging.getLogger("kyproxy")

TERMINAL = ("ok", "error", "cancelled", "skipped")


class CellModel:
    """Proxy-side authoritative copy of the kernel-owned part of a cell."""

    def __init__(self, cid: str):
        self.cid = cid
        self.outputs: list[dict] = []          # nbformat dicts, stream text as str
        self.display_ids: list[str | None] = []
        self.execution_count = None
        self.state = "idle"
        self.cursor = 0                       # terminal cursor in last stream output
        self.pending_clear = False            # clear_output(wait=True)
        self.stream_dirty = False             # last stream text not yet flushed to Y
        self.ymap: Map | None = None          # last Y Map the cell lived in
        self.finished_at = 0.0


class Execution:
    def __init__(self, req_id: str, cid: str):
        self.req_id, self.cid = req_id, cid
        self.msg_id: str | None = None
        self.reply: asyncio.Future = asyncio.get_running_loop().create_future()
        self.idle = asyncio.Event()


class KernelProxy:
    def __init__(self, port: int, nb_room: str, kernel_room: str, *,
                 kernel_name: str = "python3", flush_interval: float = 0.0,
                 terminal: bool = True, reconcile_grace: float = 3.0):
        self.port, self.nb_room, self.kernel_room = port, nb_room, kernel_room
        self.kernel_name = kernel_name
        self.flush_interval = flush_interval
        self.terminal = terminal              # apply \r/\b in the proxy (False: append raw text)
        self.reconcile_grace = reconcile_grace
        self.models: dict[str, CellModel] = {}
        self.msg_to_cell: OrderedDict[str, str] = OrderedDict()   # msg_id -> cell id (bounded)
        self.displays: dict[str, set[str]] = {}                    # display_id -> cell ids
        self.pending_shell: dict[str, asyncio.Future] = {}
        self.current: Execution | None = None
        self._exec_task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._cells_changed = False
        self._flush_handles: dict[str, asyncio.TimerHandle] = {}
        self._last_flush: dict[str, float] = {}
        self._tasks: list[asyncio.Task] = []
        self._conns: list[DocConnection] = []
        self.stats = {"y_updates": 0, "y_bytes": 0}

    # ------------------------------------------------------------------ setup
    async def _open(self, room):
        c = DocConnection(self.port, room)
        self._conns.append(c)
        return await c.open()

    async def __aenter__(self):
        nbdoc = await self._open(self.nb_room)
        kdoc = await self._open(self.kernel_room)
        self.nb = YNotebook(nbdoc)
        self.k = KernelDoc(kdoc)
        self.nbdoc, self.kdoc = nbdoc, kdoc
        nbdoc.observe(self._count_update)
        self._recover()
        self._write_status(execution_state="starting", kernel_name=self.kernel_name)
        self.km = AsyncKernelManager(kernel_name=self.kernel_name)
        await self.km.start_kernel()
        await self._connect_client()
        self.nb.ycells.observe_deep(self._on_nb_change)
        for r in ("queue", "control", "requests", "stdin_replies"):
            getattr(self.k, r).observe_deep(self._on_k_change)
        for coro in (self._scheduler(), self._liveness()):
            self._tasks.append(asyncio.create_task(coro))
        self._wake.set()
        return self

    async def _connect_client(self):
        self.kc = self.km.client()
        self.kc.start_channels()
        await self.kc.wait_for_ready(timeout=30)
        self._chan_tasks = [asyncio.create_task(c()) for c in (self._iopub_loop, self._shell_loop, self._stdin_loop)]
        self._write_status(execution_state="idle")

    async def __aexit__(self, *exc):
        for t in [*self._tasks, *self._chan_tasks, *([self._exec_task] if self._exec_task else [])]:
            t.cancel()
        self.kc.stop_channels()
        await self.km.shutdown_kernel(now=True)
        for c in self._conns:
            await c.close()

    def _recover(self):
        """A previous proxy (and its kernel) may have died: clean up its state."""
        with self._ktxn():
            if "generation" in self.k.status:
                # new kernel => new generation; pending requests aimed at the old one are cancelled
                self.k.status["generation"] = int(self.k.status["generation"]) + 1
            else:
                self.k.status["generation"] = 0
            for rid, rec in list(self.k.queue.items()):
                if rec.get("state") == "running":
                    self.k.queue[rid] = {**rec, "state": "cancelled", "reason": "proxy restarted"}
            for rid in list(self.k.stdin.keys()):
                del self.k.stdin[rid]
        with self._ntxn():
            for c in self.nb.ycells:
                if c.get("execution_state") == "running":
                    c["execution_state"] = "idle"

    def _count_update(self, ev):
        self.stats["y_updates"] += 1
        self.stats["y_bytes"] += len(ev.update)

    # -------------------------------------------------------------- observers
    def _on_nb_change(self, events):
        if events[0].transaction.origin == PROXY_ORIGIN:
            return
        self._cells_changed = True
        self._wake.set()

    def _on_k_change(self, events):
        if events[0].transaction.origin == PROXY_ORIGIN:
            return
        self._wake.set()

    # ------------------------------------------------------------- doc writes
    def _ktxn(self):
        return self.kdoc.transaction(origin=PROXY_ORIGIN)

    def _ntxn(self):
        return self.nbdoc.transaction(origin=PROXY_ORIGIN)

    def _write_status(self, **kw):
        with self._ktxn():
            for k, v in kw.items():
                if self.k.status.get(k) != v:
                    self.k.status[k] = v

    def _set_req(self, rid: str, **kw):
        rec = self.k.queue.get(rid)
        if rec is None:
            return
        with self._ktxn():
            self.k.queue[rid] = {**rec, **kw}

    def _find_ycell(self, cid: str) -> Map | None:
        for c in self.nb.ycells:
            if c.get("id") == cid:
                return c
        return None

    def _ycell_for(self, m: CellModel) -> tuple[Map | None, bool]:
        """Resolve the Y Map of a cell by *id*. Returns (map, moved).

        JupyterLab moves cells by delete + re-insert of a clone, so a cached
        Map can be dead (its content is GC'd: reads raise KeyError)."""
        if m.ymap is not None:
            try:
                if m.ymap.get("id") == m.cid:
                    return m.ymap, False
            except Exception:
                pass
        y = self._find_ycell(m.cid)
        moved = m.ymap is not None and y is not None
        m.ymap = y
        return y, moved

    @staticmethod
    def _to_y(out: dict) -> Map:
        out = dict(out)
        if out.get("output_type") == "stream":
            out["text"] = Text(out["text"])
        return Map(out)

    def _write_all(self, m: CellModel, y: Map):
        with self._ntxn():
            if not isinstance(y.get("outputs"), Array):
                y["outputs"] = Array()
            y["outputs"].clear()
            y["outputs"].extend([self._to_y(o) for o in m.outputs])
            y["execution_count"] = m.execution_count
            y["execution_state"] = m.state
        m.stream_dirty = False

    def _sync(self, m: CellModel, op: str, *args):
        """Apply one incremental change of the model to Y."""
        y, moved = self._ycell_for(m)
        if y is None:
            return  # cell deleted: outputs are dropped (see README)
        if moved:
            self._write_all(m, y)
            return
        with self._ntxn():
            youts = y["outputs"]
            if op == "append":
                youts.append(self._to_y(m.outputs[-1]))
            elif op == "replace":
                youts[args[0]] = self._to_y(m.outputs[args[0]])
            elif op == "clear":
                youts.clear()
            elif op == "stream":
                last = youts[len(youts) - 1] if len(youts) else None
                if last is None or not isinstance(last.get("text"), Text):
                    youts.clear()
                    youts.extend([self._to_y(o) for o in m.outputs])
                else:
                    yt: Text = last["text"]
                    old, new = str(yt), m.outputs[-1]["text"]
                    p = 0
                    n = min(len(old), len(new))
                    while p < n and old[p] == new[p]:
                        p += 1
                    if p < len(old):
                        del yt[p:len(old)]
                    if p < len(new):
                        yt.insert(p, new[p:])
                m.stream_dirty = False
            elif op == "meta":
                y["execution_count"] = m.execution_count
                if y.get("execution_state") != m.state:
                    y["execution_state"] = m.state

    def _flush(self, m: CellModel):
        h = self._flush_handles.pop(m.cid, None)
        if h:
            h.cancel()
        if m.stream_dirty:
            self._last_flush[m.cid] = time.monotonic()
            self._sync(m, "stream")

    # ------------------------------------------------------- output handling
    def _model(self, cid: str) -> CellModel:
        if cid not in self.models:
            self.models[cid] = CellModel(cid)
        return self.models[cid]

    def _maybe_pending_clear(self, m: CellModel):
        if m.pending_clear:
            m.pending_clear = False
            self._clear(m)

    def _clear(self, m: CellModel):
        self._flush(m)
        m.outputs.clear()
        m.display_ids.clear()
        m.cursor = 0
        m.stream_dirty = False
        self._sync(m, "clear")

    def _add_output(self, m: CellModel, out: dict, display_id: str | None = None):
        self._flush(m)
        self._maybe_pending_clear(m)
        m.outputs.append(out)
        m.display_ids.append(display_id)
        if display_id:
            self.displays.setdefault(display_id, set()).add(m.cid)
        self._sync(m, "append")

    def _stream(self, m: CellModel, name: str, text: str):
        if m.pending_clear:
            self._flush(m)
            self._maybe_pending_clear(m)
        last = m.outputs[-1] if m.outputs else None
        if last is None or last.get("output_type") != "stream" or last["name"] != name:
            self._flush(m)
            new, m.cursor = process_text("", 0, text) if self.terminal else (text, len(text))
            m.outputs.append({"output_type": "stream", "name": name, "text": new})
            m.display_ids.append(None)
            self._sync(m, "append")
            return
        if self.terminal:
            last["text"], m.cursor = process_text(last["text"], m.cursor, text)
        else:
            last["text"] += text
        m.stream_dirty = True
        if self.flush_interval <= 0:
            self._flush(m)
            return
        due = self._last_flush.get(m.cid, 0) + self.flush_interval
        now = time.monotonic()
        if now >= due:
            self._flush(m)
        elif m.cid not in self._flush_handles:
            loop = asyncio.get_running_loop()
            self._flush_handles[m.cid] = loop.call_later(due - now, self._flush, m)

    def _handle_iopub(self, msg):
        mt, content = msg["msg_type"], msg["content"]
        parent = msg.get("parent_header", {}).get("msg_id")
        if mt == "status":
            state = content["execution_state"]
            if state in ("busy", "idle", "starting"):
                self._write_status(execution_state=state)
            if self.current and parent == self.current.msg_id and state == "idle":
                self.current.idle.set()
            return
        if mt == "update_display_data":
            did = content.get("transient", {}).get("display_id")
            for cid in self.displays.get(did, ()):
                m = self.models[cid]
                for i, d in enumerate(m.display_ids):
                    if d == did:
                        m.outputs[i] = {"output_type": "display_data", "data": content["data"],
                                        "metadata": content.get("metadata", {})}
                        self._flush(m)
                        self._sync(m, "replace", i)
            return
        cid = self.msg_to_cell.get(parent)
        if cid is None:
            return
        m = self._model(cid)
        if mt == "execute_input":
            m.execution_count = content.get("execution_count")
            self._sync(m, "meta")
        elif mt == "stream":
            self._stream(m, content["name"], content["text"])
        elif mt in ("display_data", "execute_result"):
            out = {"output_type": mt, "data": content["data"], "metadata": content.get("metadata", {})}
            if mt == "execute_result":
                out["execution_count"] = content.get("execution_count")
            self._add_output(m, out, content.get("transient", {}).get("display_id"))
        elif mt == "error":
            self._add_output(m, {"output_type": "error", "ename": content["ename"],
                                 "evalue": content["evalue"], "traceback": content["traceback"]})
        elif mt == "clear_output":
            if content.get("wait"):
                self._flush(m)
                m.pending_clear = True
            else:
                self._clear(m)

    # ------------------------------------------------------------ zmq loops
    async def _iopub_loop(self):
        while True:
            msg = await self.kc.get_iopub_msg()
            try:
                self._handle_iopub(msg)
            except Exception:
                log.exception("iopub handling failed")

    async def _shell_loop(self):
        while True:
            msg = await self.kc.get_shell_msg()
            fut = self.pending_shell.pop(msg["parent_header"].get("msg_id"), None)
            if fut and not fut.done():
                fut.set_result(msg)

    async def _stdin_loop(self):
        while True:
            msg = await self.kc.get_stdin_msg()
            if msg["msg_type"] != "input_request":
                continue
            parent = msg["parent_header"].get("msg_id")
            rid = msg["header"]["msg_id"]
            with self._ktxn():
                self.k.stdin[rid] = {"cell_id": self.msg_to_cell.get(parent),
                                     "prompt": msg["content"]["prompt"],
                                     "password": msg["content"].get("password", False),
                                     "exec_req": self.current.req_id if self.current else None}

    def _shell_request(self, msg_id: str) -> asyncio.Future:
        fut = asyncio.get_running_loop().create_future()
        self.pending_shell[msg_id] = fut
        return fut

    # ------------------------------------------------------------ scheduler
    async def _scheduler(self):
        while True:
            await self._wake.wait()
            self._wake.clear()
            try:
                await self._handle_control()
                self._handle_requests()
                self._handle_stdin_replies()
                if self._cells_changed:
                    self._cells_changed = False
                    self._reconcile()
                self._maybe_start_next()
            except Exception:
                log.exception("scheduler step failed")

    def _reconcile(self):
        """A remote edit touched cells: re-home outputs of cells that were moved
        (deleted + re-inserted clone) while we own their outputs."""
        now = time.monotonic()
        for m in self.models.values():
            if m.ymap is None:
                continue
            if m.state != "running" and now - m.finished_at > self.reconcile_grace:
                # outside the grace window the user owns the cell again (e.g. they
                # may have cleared outputs before moving it); stop tracking.
                continue
            y, moved = self._ycell_for(m)
            if moved:
                self._write_all(m, y)

    def _pending(self) -> list[tuple[str, dict]]:
        gen = int(self.k.status.get("generation", 0))
        out = []
        # list(): iterating a Y Map holds a (origin-less) transaction open, and
        # writing with another origin inside it raises.
        for rid, rec in list(self.k.queue.items()):
            if rec.get("state") == "pending":
                if int(rec.get("gen", 0)) != gen:
                    # written by a client that had not yet seen the restart
                    self._set_req(rid, state="cancelled", reason="stale generation")
                    continue
                out.append((rid, rec))
        out.sort(key=lambda kv: sort_key(kv[1]))
        return out

    def _maybe_start_next(self):
        if self._exec_task and not self._exec_task.done():
            return
        pending = self._pending()
        if pending:
            rid, rec = pending[0]
            self._exec_task = asyncio.create_task(self._run(rid, rec))

    def _cancel_pending(self, reason: str):
        with self._ktxn():
            for rid, rec in list(self.k.queue.items()):
                if rec.get("state") == "pending":
                    self.k.queue[rid] = {**rec, "state": "cancelled", "reason": reason}

    async def _run(self, rid: str, rec: dict):
        cid = rec["cell_id"]
        y = self._find_ycell(cid)
        if y is None or y.get("cell_type") != "code":
            self._set_req(rid, state="skipped")
            self._wake.set()
            return
        source = str(y["source"])  # snapshot at claim time
        ex = self.current = Execution(rid, cid)
        m = self._model(cid)
        m.outputs, m.display_ids, m.cursor, m.pending_clear = [], [], 0, False
        m.execution_count, m.state, m.ymap = None, "running", y
        with self._ktxn():
            self.k.status["last_seq"] = max(int(self.k.status.get("last_seq", 0)), int(rec["seq"]))
            self.k.queue[rid] = {**rec, "state": "running", "started": time.time()}
        self._write_all(m, y)
        state = "cancelled"
        try:
            ex.msg_id = self.kc.execute(source, allow_stdin=True, stop_on_error=True)
            self.msg_to_cell[ex.msg_id] = cid
            while len(self.msg_to_cell) > 1000:
                self.msg_to_cell.popitem(last=False)
            reply = await self._wait_reply(ex)
            try:
                await asyncio.wait_for(ex.idle.wait(), 5)  # let trailing iopub land
            except asyncio.TimeoutError:
                log.warning("no idle for %s", ex.msg_id)
            c = reply["content"]
            state = "ok" if c["status"] == "ok" else "error"
            m.execution_count = c.get("execution_count", m.execution_count)
            if state == "error":
                self._cancel_pending("previous cell failed")  # stop_on_error semantics
        finally:
            self._flush(m)
            m.state = "idle"
            m.finished_at = time.monotonic()
            self._sync(m, "meta")
            with self._ktxn():  # input prompts of this execution are moot now
                for srid, p in list(self.k.stdin.items()):
                    if p.get("exec_req") == rid:
                        del self.k.stdin[srid]
            self._set_req(rid, state=state, finished=time.time())
            self.current = None
            self._wake.set()

    async def _wait_reply(self, ex: Execution):
        fut = self._shell_request(ex.msg_id)
        return await fut

    # ------------------------------------------------- control / requests
    async def _handle_control(self):
        for what in ("interrupt", "restart"):
            tok = self.k.control.get(what)
            if tok and self.k.control_ack.get(what) != tok:
                with self._ktxn():
                    self.k.control_ack[what] = tok
                if what == "interrupt":
                    await self.km.interrupt_kernel()
                else:
                    await self._restart()

    async def _restart(self):
        self._write_status(execution_state="restarting")
        self._cancel_pending("kernel restarted")
        if self._exec_task and not self._exec_task.done():
            self._exec_task.cancel()
            try:
                await self._exec_task
            except BaseException:
                pass
        for t in self._chan_tasks:
            t.cancel()
        self.kc.stop_channels()
        for fut in self.pending_shell.values():
            fut.cancel()
        self.pending_shell.clear()
        with self._ktxn():
            self.k.status["generation"] = int(self.k.status.get("generation", 0)) + 1
            for rid in list(self.k.stdin.keys()):
                del self.k.stdin[rid]
        await self.km.restart_kernel(now=True)
        await self._connect_client()

    def _handle_requests(self):
        for rid, req in list(self.k.requests.items()):
            if req.get("type") == "complete":
                msg_id = self.kc.complete(req["code"], int(req["cursor_pos"]))
                fut = self._shell_request(msg_id)
                fut.add_done_callback(lambda f, rid=rid: self._reply(rid, f))
            else:
                self._write_reply(rid, {"status": "error", "error": f"unknown type {req.get('type')}"})
            with self._ktxn():
                del self.k.requests[rid]

    def _reply(self, rid, fut):
        if fut.cancelled():
            self._write_reply(rid, {"status": "error", "error": "cancelled"})
        else:
            c = fut.result()["content"]
            self._write_reply(rid, {"status": c.get("status", "ok"), "content": c})

    def _write_reply(self, rid, value):
        with self._ktxn():
            self.k.replies[rid] = value

    def _handle_stdin_replies(self):
        for rid, value in list(self.k.stdin_replies.items()):
            pending = self.k.stdin.get(rid)
            with self._ktxn():
                del self.k.stdin_replies[rid]
                if pending is not None:
                    del self.k.stdin[rid]
            if pending is None:
                continue  # already answered by someone else, or stale
            self.kc.input(value)
            cid = pending.get("cell_id")
            if cid:
                m = self._model(cid)
                echo = pending["prompt"] + ("" if pending.get("password") else value) + "\n"
                self._add_output(m, {"output_type": "stream", "name": "stdin", "text": echo})

    async def _liveness(self):
        while True:
            await asyncio.sleep(1)
            if self.k.status.get("execution_state") not in ("restarting", "starting"):
                if not await self.km.is_alive():
                    self._write_status(execution_state="dead")
