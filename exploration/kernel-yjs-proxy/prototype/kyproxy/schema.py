"""Shared schema for the side "kernel session" doc, plus small helpers.

Side doc roots (all Y Maps):

  status        execution_state: starting|idle|busy|restarting|dead
                generation: int  (bumped on every restart)
                last_seq: int    (highest execution seq the proxy has claimed)
                kernel_name, language
  queue         req_id -> {cell_id, seq, client, gen, state, ...}
                state: pending -> running -> ok|error|cancelled|skipped
                Written by clients (create, state=pending), afterwards only by the proxy.
  control       "interrupt" | "restart" -> token (str). A new token is an intent.
  control_ack   "interrupt" | "restart" -> last token handled by the proxy.
  requests      req_id -> {type: "complete", code, cursor_pos, client}
  replies       req_id -> {status, content, ...}   (proxy writes; requester deletes)
  stdin         req_id -> {cell_id, prompt, password, exec_req}  (pending input prompts)
  stdin_replies req_id -> str                      (any client answers)

The notebook doc is an unmodified jupyter_ydoc YNotebook. The proxy writes
`outputs`, `execution_count` and `execution_state` ('running'|'idle') of cells.
"""

from __future__ import annotations

import uuid

from pycrdt import Doc, Map

ROOTS = ("status", "queue", "control", "control_ack", "requests", "replies", "stdin", "stdin_replies")
PROXY_ORIGIN = "kernel-proxy"


class KernelDoc:
    def __init__(self, doc: Doc):
        self.doc = doc
        for r in ROOTS:
            setattr(self, r, doc.get(r, type=Map))

    # ---- client side helpers -------------------------------------------------
    def next_seq(self) -> int:
        seqs = [int(v["seq"]) for v in self.queue.values()]
        return max([int(self.status.get("last_seq", 0)), *seqs]) + 1

    def request_execution(self, cell_ids: list[str]) -> list[str]:
        """Enqueue cells in the given order (one transaction, consecutive seqs)."""
        client = self.doc.client_id
        gen = int(self.status.get("generation", 0))
        ids = []
        with self.doc.transaction():
            seq = self.next_seq()
            for i, cid in enumerate(cell_ids):
                rid = uuid.uuid4().hex
                self.queue[rid] = {"cell_id": cid, "seq": seq + i, "client": client,
                                   "gen": gen, "state": "pending"}
                ids.append(rid)
        return ids

    def request(self, type_: str, **content) -> str:
        rid = uuid.uuid4().hex
        self.requests[rid] = {"type": type_, "client": self.doc.client_id, **content}
        return rid

    def control_intent(self, what: str) -> str:
        tok = uuid.uuid4().hex
        self.control[what] = tok
        return tok


def sort_key(rec: dict) -> tuple:
    """Total order of pending executions: (seq, Y client id)."""
    return (int(rec["seq"]), int(rec["client"]))


def process_text(text: str, cursor: int, new: str) -> tuple[str, int]:
    """Apply terminal controls (\\r, \\b, \\n) of `new` onto `text` at `cursor`.

    Port of JupyterLab outputarea Private.processText: with RTC today the
    *frontend* does this before writing to the Y doc; with a kernel proxy the
    proxy has to own terminal semantics.
    """
    out = list(text)
    i = cursor
    for ch in new:
        if ch == "\r":
            while i > 0 and out[i - 1] != "\n":
                i -= 1
        elif ch == "\b":
            if i > 0 and out[i - 1] != "\n":
                del out[i - 1]
                i -= 1
        elif ch == "\n":
            out.append("\n")
            i = len(out)
        else:
            if i < len(out):
                out[i] = ch
            else:
                out.append(ch)
            i += 1
    return "".join(out), i
