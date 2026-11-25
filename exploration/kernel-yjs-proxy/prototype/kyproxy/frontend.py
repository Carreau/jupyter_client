"""Headless "frontend": a Y client of the notebook + kernel docs, nothing else.
It never talks to the kernel; it only writes intents and reads state."""

from __future__ import annotations

import asyncio
import time

from jupyter_ydoc import YNotebook
from pycrdt import Map

from .schema import KernelDoc
from .transport import DocConnection


async def wait_until(pred, timeout: float = 10.0, interval: float = 0.005):
    t0 = time.monotonic()
    while True:
        v = pred()
        if v:
            return v
        if time.monotonic() - t0 > timeout:
            raise TimeoutError("condition not met")
        await asyncio.sleep(interval)


class Frontend:
    def __init__(self, port: int, nb_room: str, kernel_room: str):
        self.port, self.nb_room, self.kernel_room = port, nb_room, kernel_room
        self._conns: list[DocConnection] = []

    async def _open(self, room):
        c = DocConnection(self.port, room)
        self._conns.append(c)
        return await c.open()

    async def __aenter__(self):
        self.nbdoc = await self._open(self.nb_room)
        self.kdoc = await self._open(self.kernel_room)
        self.nb = YNotebook(self.nbdoc)
        self.k = KernelDoc(self.kdoc)
        return self

    async def __aexit__(self, *exc):
        for c in self._conns:
            await c.close()

    # ---- notebook editing ----------------------------------------------------
    def add_cell(self, source: str, cell_type: str = "code") -> str:
        cell = {"cell_type": cell_type, "source": source, "metadata": {}}
        if cell_type == "code":
            cell.update(outputs=[], execution_count=None)
        ycell = self.nb.create_ycell(cell)
        self.nb.ycells.append(ycell)
        return ycell["id"]

    def cell(self, cid: str) -> dict:
        for c in self.nb.ycells:
            if c.get("id") == cid:
                return c.to_py()
        raise KeyError(cid)

    def cell_ids(self) -> list[str]:
        return [c["id"] for c in self.nb.ycells]

    def move_cell(self, frm: int, to: int):
        """Exactly what JupyterLab's YNotebook.moveCells does: delete + insert a clone."""
        clone = self.nb.ycells[frm].to_py()
        with self.nbdoc.transaction():
            del self.nb.ycells[frm]
            self.nb.ycells.insert(to if frm > to else to, self.nb.create_ycell(clone))

    # ---- kernel intents ------------------------------------------------------
    def execute(self, *cids: str) -> list[str]:
        return self.k.request_execution(list(cids))

    def run_all(self) -> list[str]:
        return self.execute(*[c["id"] for c in self.nb.ycells if c["cell_type"] == "code"])

    def req(self, rid: str) -> dict:
        return self.k.queue.get(rid) or {}

    async def wait_done(self, *rids: str, timeout: float = 20):
        await wait_until(lambda: all(self.req(r).get("state") in ("ok", "error", "cancelled", "skipped")
                                     for r in rids), timeout)

    def interrupt(self):
        return self.k.control_intent("interrupt")

    def restart(self):
        return self.k.control_intent("restart")

    async def complete(self, code: str, cursor_pos: int | None = None) -> tuple[dict, float]:
        t0 = time.perf_counter()
        rid = self.k.request("complete", code=code, cursor_pos=len(code) if cursor_pos is None else cursor_pos)
        rep = await wait_until(lambda: self.k.replies.get(rid), 10, interval=0.001)
        ms = (time.perf_counter() - t0) * 1000
        del self.k.replies[rid]  # requester garbage-collects its reply
        return rep, ms

    def answer_input(self, rid: str, value: str):
        self.k.stdin_replies[rid] = value

    def outputs_text(self, cid: str) -> str:
        out = []
        for o in self.cell(cid).get("outputs", []):
            if o["output_type"] == "stream":
                out.append(o["text"])
            elif o["output_type"] in ("execute_result", "display_data"):
                out.append(o["data"].get("text/plain", ""))
            elif o["output_type"] == "error":
                out.append(f"{o['ename']}: {o['evalue']}")
        return "".join(out)
