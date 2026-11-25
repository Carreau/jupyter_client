# Exploration brief: kernel as a Yjs collaborator ("ZMQ <-> Yjs proxy")

Goal (from the user, a Jupyter core dev): explore an architecture where a proxy sits between a Jupyter
kernel (ZMQ) and the JupyterLab real-time-collaboration Y document. Proxy + kernel act as *just another
collaborator* on the notebook Y doc. The frontend no longer opens /api/kernels/<id>/channels for
execution; it expresses intent (e.g. "please execute this cell") in the Y doc, the proxy picks it up,
sends execute_request over ZMQ, and writes outputs / execution_count / state back as CRDT edits.
Closing the browser mid-execution must not lose outputs; reopening just syncs state.

Constraints / user answers:
- Exploration + design for discussion with other people. Multiple alternative solutions welcome.
- Schema may change, or a side "kernel/meta" Y document may be added. Prefer minimal changes to the
  notebook schema, but exploring is fine.
- Execution queue as document state: e.g. a pending-execution counter on cells, with client-id or
  timestamp tiebreak; user doesn't care which, any sound design is fine.
- Non-document stuff (complete, inspect, is_complete, kernel_info, history, interrupt, restart, status)
  may live in a side Y document. If something truly needs a direct interactive kernel connection
  (e.g. maybe debugger), keeping a websocket is acceptable — but justify it.
- stdin/input(): open question, explore.
- Widgets: hope that common widgets can work purely server side so state survives browser close/reopen.
- Volume (streaming outputs, progress bars) is a known issue; the server is the single source of
  truth, so assume more aggressive trimming/GC is possible; explore.
- Trust: assume good faith. The proxy is the only client allowed to modify kernel-owned state.
- No UI change: user picks a kernel as today; it "magically" connects via Yjs instead of ZMQ websocket.

Environment:
- Reference repos (shallow clones, read-only, shared): /home/user/refs/{jupyter_ydoc, jupyter-collaboration,
  jupyter-server-nbmodel, jupyverse, pycrdt, pycrdt-websocket, jupyterlab (sparse: packages/notebook,
  cells, outputarea, services, apputils, docregistry, console, debugger, completer, tooltip, rendermime,
  codemirror)}. Clone more public repos with `git clone --depth 1 https://github.com/<org>/<repo>` into
  /home/user/refs if needed (no auth; private/nonexistent repos fail).
- User's forks (writable): /home/user/{ipykernel, jupyter_client, jupyter_core, tqdm, jupyterlab(=binder example, not the real one)}.
- Python venv: /home/user/venv (pycrdt 0.14, pycrdt-websocket 0.16, jupyter_client 8.10, ipykernel 7.4,
  jupyter_ydoc 4.2, pytest, pytest-asyncio). Node 22 available.
- Write your notes as Markdown in /home/user/design/notes/<your-topic>.md. Cite file paths + line numbers
  from refs for claims about existing code. Do not commit or push anything.
