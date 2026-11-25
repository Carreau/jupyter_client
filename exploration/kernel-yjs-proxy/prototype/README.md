# Kernel-as-Y-collaborator prototype

A working Python prototype of the "ZMQ <-> Yjs proxy" architecture from `../BRIEF.md`.
It's an exploration artifact. It exists to surface design issues, not to ship.

```
 headless "frontends"            room server                       proxy
 (pycrdt Doc + Provider)   <-->  pycrdt-websocket            <-->  pycrdt Doc + Provider (x2)
 write intents, read state       WebsocketServer over              + jupyter_client AsyncKernelManager
                                 `websockets` (knows nothing          / AsyncKernelClient  <-ZMQ->  ipykernel
                                 about kernels)
 rooms:  /nb/<path>      = unmodified jupyter_ydoc YNotebook
         /kernel/<path>  = side "kernel session" doc (schema in kyproxy/schema.py)
```

The proxy is a websocket Y client like any browser. The room server has no kernel-specific
code at all.

## Running

```bash
cd /home/user/design/prototype
/home/user/venv/bin/pip install websockets tqdm   # websockets is required; tqdm only for one test
/home/user/venv/bin/python -m pytest -q -s        # -s prints the measurements
```

Files:
- `kyproxy/transport.py`: room server and `connect_doc` (Provider over `websockets`). About 130 lines.
- `kyproxy/schema.py`: side-doc layout, ordering key, terminal-control text processing.
- `kyproxy/proxy.py`: the proxy. About 570 lines.
- `kyproxy/frontend.py`: the headless client used by the tests.
- `tests/test_proxy.py`: 26 tests, including the measurements.

**Status: all 26 tests pass.** I ran the full suite 3 times with no flakes, about 40-55 s per run. Each test starts a real ipykernel.

## Design (what the prototype does)

### Side doc `/kernel/<path>`
All roots are Y Maps:

| root | writer | content |
|---|---|---|
| `status` | proxy | `execution_state` (starting/idle/busy/restarting/dead), `generation`, `last_seq`, `kernel_name` |
| `queue` | client creates, then proxy only | `req_id -> {cell_id, seq, client, gen, state}`. `state` goes pending -> running -> ok/error/cancelled/skipped |
| `control` / `control_ack` | client / proxy | `"interrupt"`/`"restart"` -> token. A token that differs from the ack is an intent; the proxy acks first, then acts. This is idempotent and survives a proxy restart. |
| `requests` / `replies` | client / proxy | Request/reply keyed by request id (completion). The proxy deletes the request; the requester deletes the reply. |
| `stdin` / `stdin_replies` | proxy / client | Pending `input()` prompts keyed by the kernel's input_request msg id. Any client can answer. |

### Execution intent and order
- Ordering: a client enqueues with `seq = max(status.last_seq, all queue seqs) + 1`. "Run all" and multi-cell runs use consecutive seqs in one transaction.
- The total order is `(seq, Y client_id)`. Concurrent enqueues that pick the same seq are tie-broken by client id. The test shows two clients choosing the same seq and the kernel running them in `sort_key` order.
- Intents live in the side doc. The notebook schema is unchanged: the proxy writes only `outputs`, `execution_count` and `execution_state` (`running`/`idle`).
- One execution in flight: the proxy sends one `execute_request` at a time and claims the lowest pending request only when the kernel is free. Source is snapshotted at *claim* time. This keeps queue semantics (cancel, stop-on-error, restart, moves) entirely in the doc, at the cost of one proxy hop between cells (around 1 ms locally).
- Errors: an `execute_reply` with status `error` makes the proxy cancel every pending request (stop_on_error).
- Restarts: a restart, or a new proxy starting against an existing doc, bumps `generation`. Pending requests stamped with an older generation are cancelled, not run, which fences off stale intents.

### Outputs
- Cells are always addressed by **cell id**, never by index or a cached Map reference (see issue 1).
- The proxy keeps an in-memory authoritative model of each cell's kernel-owned fields and writes incremental changes to Y: append, replace, clear, stream-diff and meta.
- Streams: the proxy applies `\r` / `\b` / `\n` terminal semantics (a port of JupyterLab's `processText`) and writes a minimal diff: common prefix, delete tail, insert new tail. Contiguous same-name streams are coalesced into the last output's `Y.Text`.
- Throttling (optional `flush_interval`): stream text accumulates in the model and is flushed to Y at most every N ms. Any non-stream output and the end of execution flush first, so ordering is preserved.
- `clear_output(wait=True)` sets a flag. The next output clears and adds in the same transaction, so collaborators see no flash of an empty cell.
- `display_id`: the proxy keeps `display_id -> {cell ids}` and a per-output display-id list in memory. `update_display_data` replaces the matching outputs in every cell. The test updates a display in cell A from cell B.
- Errors become error outputs; `execution_count` is set from `execute_input` (early) and again from the reply.

### Threading and re-entrancy
- Single asyncio loop. Y observers (`observe_deep` on `ycells` and on each side-doc root) never write. They check `events[0].transaction.origin` and ignore the proxy's own origin (`"kernel-proxy"`), then set an `asyncio.Event`.
- A scheduler task does all the work: control, requests, stdin replies, reconcile, then start next.
- Separate tasks read iopub, shell (futures keyed by parent msg_id) and stdin.

## Measurements (localhost, one kernel, 1-2 clients)

| what | result |
|---|---|
| intent -> `ok` state visible in the requesting client (trivial cell `1`) | **median 11.5 ms** (min 10.6, max 16.9). Includes 2 Y hops and the 5 ms test polling. |
| completion round trip through Y maps (`import o` -> matches) | **median ~10 ms**, min 8 ms. The first call is 70-250 ms (jedi warm-up). |
| 1000 `\r` progress updates (`write + flush`, 2 ms apart), proxy applies `\r`, no throttle | 1004 Y updates, 68 KB of update traffic, final doc +248 B |
| same, flush at most every 50 ms | 57 Y updates, 3.9 KB, +245 B |
| same, flush at most every 100 ms | 31 Y updates, 2.1 KB, +245 B |
| same, proxy does **not** apply `\r` (raw append, as a naive writer would) | 1004 updates, 59 KB, **final doc +33 KB** (grows linearly with updates, forever) |
| raw append + 100 ms throttle | 31 updates, 34 KB, +33 KB (fewer updates, same doc bloat) |
| real tqdm, default `mininterval=0.1` | about 27 Y updates for 1000 iterations (tqdm already throttles) |
| real tqdm, `mininterval=0` | 1006 updates without throttle, 32 with a 100 ms proxy throttle |

Observations:
- With `\r` processing, the final doc stays tiny even after 1000 rewrites. pycrdt's GC drops deleted text content, and the doc's re-encoded size (669 B) is about the same as re-encoding the JSON from scratch (705 B).
- The cost of unthrottled progress output is **traffic and the update log, not doc size**. 68 KB is broadcast to *every* connected client. Any append-only update store (a pycrdt `YStore` records each update) also keeps all 1004 updates for a 250 B state, so it needs compaction.
- A 50-100 ms proxy-side throttle cuts updates by 20-30x and adds at most that much latency to output.
- Applying `\r` in the proxy is **mandatory**, not an optimisation. Today the JupyterLab *frontend* applies it before writing to Y (`packages/outputarea/src/model.ts:540-610`, `processText`). Once the proxy is the writer, it has to own terminal semantics, as `jupyter-server-nbmodel` already does (`jupyter_server_nbmodel/actions.py:240`).

## Tests (all passing)

- One-cell execution.
- Two clients enqueueing concurrently: deterministic `(seq, client)` order, and the equal-seq tie is checked.
- Run-all keeps document order and execution counts 1..7.
- Cell moved while queued **and** while running, with JupyterLab-style delete+clone moves.
- Cell moved 10 times while it streams 200 lines every 5 ms: complete output, no duplicate cell.
- A client disconnects mid-execution and a brand-new client later sees the complete outputs and count.
- An error cancels queued requests.
- Interrupt: a `KeyboardInterrupt` output appears, the next request is cancelled and the ack token is recorded.
- Restart: running and queued requests are cancelled, `generation` becomes 1, globals are gone and `execution_count` starts again at 1.
- A stale-generation intent is cancelled.
- `input()` round trip.
- Interrupt while `input()` is pending: the prompt is cleaned up.
- `display_id` update across cells, and `clear_output(wait=True)` loop.
- Completion round-trip timing; requests and replies are GC'd.
- Progress volume (5 configurations) and real tqdm volume (4 configurations).
- After the grace period, "clear outputs then move" keeps the outputs cleared.
- Proxy crash and restart: orphaned `running` request and cell state are recovered, and a pending request is fenced off.

## Design issues discovered while prototyping

### A. Notebook model / schema

1. **JupyterLab moves a cell by delete + re-insert of a clone.** See `jupyter_ydoc/javascript/src/ynotebook.ts:252-266`, which has `// FIXME we need to use yjs move feature`.
   - A cell's Y Map identity is not stable, so the proxy cannot hold a reference to "the cell it is executing".
   - pycrdt has no Map identity comparison (`m1 == m2` is False even for the same Map), and writing to a deleted Map raises `KeyError` because its content was GC'd.
   - Worse, there is a **real CRDT race**. Outputs the proxy appends to the old Map, concurrently with the client's clone, are lost: the clone was made from the client's stale snapshot.
   - The prototype keeps an authoritative in-memory copy of each cell's kernel-owned fields. On every remote cell-array change it re-resolves cells by id and, if a cell's Map died, re-homes the outputs (`_reconcile`). The fast-stream-while-moving test passes only because of this.
   - That raises a new question: how long does the proxy "own" the outputs? A user who clears outputs and then moves the cell must not get them back. The prototype uses a grace window (3 s after finish), which is a heuristic.
   - Root-cause fixes: (a) a real move (Yjs has no array move; yrs has `move_to` but pycrdt does not expose it), or (b) **store kernel-owned outputs outside the cell Map**, for example `outputs: Map<cell_id, Array>` in the notebook doc or the side doc. Moves then never touch them. That is a schema change, but arguably the cleanest single change this architecture could ask for.
2. **Duplicate cell ids.** Two clients moving the same cell concurrently produce *two* cells with the same id. I verified this with pycrdt: `['c2','c1','c1']`. Copy/paste can do the same. id-based addressing then becomes ambiguous; the prototype picks the first match. `jupyter_ydoc` only dedups on `get()` (`ynotebook.py`, `_get`).
3. **`execution_state` is only `'running' | 'idle'`** (`jupyter_ydoc/javascript/src/api.ts:537`). "Queued" (`[*]` before it runs) cannot be expressed in the notebook doc, so the frontend must join the cell with the side-doc queue to render it. Either extend the enum (for example `'queued'`) or accept the join.
4. **No atomicity across two docs.** "Request running" (side doc) and "cell `execution_state=running` + outputs cleared" (notebook) are two transactions in two rooms.
   - Clients transiently see inconsistent states.
   - After a proxy crash the inconsistency is permanent, which is why `_recover()` exists: it resets orphaned `running` cells and requests and clears stdin prompts.
   - Putting the queue in the notebook doc (for example a `state`/`kernel` root next to `cells`) would make claim + clear atomic.
5. **Clearing outputs at claim time vs request time.** In JupyterLab today, outputs clear the instant you press Shift-Enter. With "only the proxy writes kernel-owned state", old outputs stay visible while the cell is queued. Options: let clients clear (bends the trust rule), or render "queued" cells dimmed.
6. **Source snapshot at claim time vs intent time.** The prototype runs the source *as it is when the kernel gets to it*. Edits made while queued are executed, which is not today's behaviour. Alternative: put the source (or a state vector) in the intent. That is cheap for typical cells.
7. **Numbers come back as floats** (`execution_count`, `seq`, `gen` are `2.0`). Every comparison needs `int()`. `jupyter_ydoc` already has `cast_all` for this.

### B. Ordering, intents, races

8. **"Deterministic order" is only deterministic among requests the proxy can see at pick time.**
   - A client that was offline (or slow) can write `seq=5` after the proxy already claimed `seq=7`. It runs next, which is fine but not "global order".
   - With `(seq, clientID)`, a run-all from A (seq 5..10) and a single run from B (seq 5) **interleave**: A5, B5, A6, ... A "batch" intent (one record with a list of cell ids) would keep run-all contiguous and also cut the number of records.
9. **No delivery acknowledgement for intents.** `pycrdt.Provider` says sends "do not confirm receipt or persistence" (`pycrdt/python/pycrdt/_provider.py:92`).
   - A browser that closes right after Shift-Enter may or may not have delivered the intent.
   - Worse, with offline persistence (y-indexeddb) an intent can be delivered *hours later* when the laptop reopens, and it would then execute.
   - The `generation` fence covers restarts only. Intents probably need an expiry the proxy can judge, such as a server-assigned session id or a TTL against server time.
10. **Who garbage-collects the queue?** Finished records are never deleted in the prototype.
    - If the proxy deletes them, an offline client loses the only place that says "your run was cancelled/failed".
    - `next_seq` must not depend on them: hence `status.last_seq`.
    - Deleted Map keys leave (small) tombstones.
11. **stop_on_error in multi-user land.** On error the proxy cancels *all* pending requests, including another user's unrelated queued cells. Kernel-side `stop_on_error` has the same blunt semantics, but it was designed for one user. It should probably be scoped to the batch or requesting client.
12. **Intent vs claim race on cancel.** Queue records are whole-value LWW. A client "un-queueing" a cell while the proxy claims it is resolved arbitrarily. The prototype avoids this by not supporting client cancel. A real design needs per-field ownership (client-owned `cancel_requested`, proxy-owned `state`), meaning nested Maps rather than JSON values.
13. **Single writer is not enforced.** Two proxies attached to the same room (two server processes, or a restarted proxy while the old one hangs) would both execute. This needs a lease or owner field with a heartbeat in `status`. Similarly, "only the proxy writes kernel-owned state" cannot be enforced by the room server without decoding Y updates and attributing them to paths.

### C. pycrdt / pycrdt-websocket API friction

14. **`Doc.observe` events carry no origin.** `TransactionEvent` only has `.update` (`pycrdt/python/pycrdt/_pycrdt.pyi:101-109`). To ignore its own edits, the proxy has to `observe_deep` each root type separately (4 roots in the side doc) and read `events[0].transaction.origin`.
15. **Exceptions in observers are swallowed** into `doc._exceptions` (`pycrdt/python/pycrdt/_base.py:409,425`). A buggy observer silently stops reacting. Combined with re-entrancy, the only sane pattern is "observer sets a flag, a task does the work". That is what the prototype does; it costs one loop hop.
16. **Iterating a Map holds a transaction open.** Writing with `transaction(origin=...)` inside `for k, v in m.items():` raises `RuntimeError: Nested transactions must have same origin as root transaction` (`pycrdt/python/pycrdt/_doc.py:115-122`). I hit this in a real test failure. You have to snapshot with `list(...)` first.
17. **anyio cancel-scope task affinity.** `Provider` and `WebsocketServer` contexts must be exited by the task that entered them. pytest-asyncio fixtures (and many server lifecycles) violate that, so the prototype wraps each connection and the server in a dedicated task (`DocConnection`, `RoomServer._main`).
18. **`WebsocketServer(auto_clean_rooms=True)` is the default.** Without a ystore, a room is deleted when its last client leaves. With a proxy permanently connected, the room lives as long as the kernel, which is arguably what we want (the server is the source of truth while the kernel runs). It is still a lifecycle change worth stating.
19. pycrdt has no websocket transport of its own. The `Channel` protocol is small, but `async for` must end with `StopAsyncIteration` on close, or the Provider task dies noisily.

### D. Outputs / volume

20. **Terminal semantics move into the proxy** (see the measurements). Without this, the doc grows by about 33 B per progress tick, forever.
21. **Throttling is cheap and very effective** (20-30x fewer updates), but it must be ordered: flush before any non-stream output, before `clear_output`, and at execution end.
22. **Traffic, not state, is the problem.** GC keeps the final doc small, but every update is broadcast to all peers and appended to any update-log store, so compaction is needed.
23. **`display_id` bookkeeping is proxy-memory only.** nbformat outputs cannot carry `transient`, and the schema forbids extra keys. After a proxy restart, `update_display_data` for an old display is dropped. Persisting `display_id -> (cell_id, output index)` in the side doc is possible, but indices shift on clear.
24. **Late outputs.** Output from background threads arriving after the execution finished is routed by parent msg_id through a bounded LRU. If the cell was re-run in the meantime, the late output lands in the *new* run's outputs. JupyterLab drops output for finished futures, so a policy choice is needed.
25. **Kernel status traffic.** ipykernel emits busy/idle for *every* shell request, so each completion request also produces 2 status writes in the side doc, broadcast to everyone. Filter status to execute requests, or debounce.

### E. stdin

26. The prompt-in-side-doc / answer-in-side-doc pattern works, including interrupt while waiting (the prompt is cleaned up when the execution ends). Open points:
    - **Which client answers?** Anyone can; first write wins and other answers are dropped. Should the prompt be shown only to the user who ran the cell (the request records `client`)?
    - **Passwords** (`getpass`): the answer travels through the shared doc and lives in update history and stores until compaction, visible to every collaborator. stdin for passwords probably needs an ephemeral, non-persisted channel (awareness-like or a direct message).
    - **Nobody connected:** the kernel blocks forever on `input()`. The proxy may need a timeout policy.
    - **Echo:** JupyterLab persists the answer as a `stream` output named `stdin` (`packages/outputarea/src/widget.ts:616-628`), and the prototype does the same (prompt + value). Who writes it is now the proxy's job.

### F. Request/reply over Y (completion)

27. About 10 ms round trip locally is fine latency-wise for completion. But every request and reply is shared state: it is broadcast to all collaborators, persisted in history, and leaves tombstones after deletion, and replies such as completion matches or inspect payloads can be large. That is the wrong medium for ephemeral, per-user RPC. Better options:
    - (a) a custom Y-protocol message type on the same websocket, next to sync/awareness, that is not part of the doc;
    - (b) awareness-like ephemeral state;
    - (c) keep the direct kernel websocket for complete/inspect/is_complete/history/debugger, and use Y only for state that must survive a browser close.

### G. Lifecycle

28. **Restart:** `AsyncKernelManager.restart_kernel` keeps ports, but the client channels and the reader tasks have to be restarted. Pending shell futures must be cancelled, and the running execution's task cancelled, with cleanup in `finally` writing the cancelled state.
29. **Proxy restart:** the prototype's policy is that pending requests written while the proxy was down are cancelled (generation bump). That is debatable. "Queue while the kernel is starting" works on first start because the bump only happens when a previous generation exists.
30. **Memory:** the proxy holds a full replica of the notebook, plus its in-memory output models, which are about 2x the outputs. A real implementation should keep models only for running cells and the grace window.
