# Kernel as a Yjs collaborator — exploration synthesis (draft)

Status: exploration draft for discussion. For the landscape / talking-points summary see `LANDSCAPE.md`. Detailed notes in `notes/`, working prototype in `prototype/`
(26 tests against a real ipykernel, passing).

| File | Content |
|---|---|
| `notes/01-current-and-prior-art.md` | jupyter_ydoc schema, jupyter-collaboration rooms, JupyterLab execution path, nbmodel, jupyverse, jupyter-server-documents, current standardisation work |
| `notes/02-execution-and-schema.md` | Execution-intent alternatives, CRDT merge experiments (pycrdt), state machine, side doc schema, recovery |
| `notes/03-side-channels.md` | complete/inspect/history, stdin, comms & widgets, debugger, display_id, consoles |
| `notes/04-frontend-integration.md` | Every frontend path that touches the kernel, integration strategies, double-writer inventory, LOC estimates |
| `prototype/README.md` | How to run, measurements, 30 "design issues discovered while prototyping" |

## 1. Target architecture

```
 Browser A ─┐                         ┌─ notebook doc  (jupyter_ydoc YNotebook, persisted)
 Browser B ─┼── Y sync (room ws) ─────┤
            │                         └─ kernel-session doc "K-doc" (per kernel, not persisted)
            │                                     ▲
            └── RAW unicast RPC (completion,      │ Y sync / in-process
                inspect, stdin replies, debug)    │
                                          ┌───────┴────────┐
                                          │  kernel peer   │  (proxy: Y client + jupyter_client)
                                          └───────┬────────┘
                                                  │ ZMQ (own session id)
                                               kernel
```

Clients never write kernel-owned state. They append *intents*; the kernel peer is the single writer of
outputs, execution_count, execution_state, kernel status, widget state.

## 2. The rule of thumb that emerged

| Kind of data | Where | Why |
|---|---|---|
| Lasting document state (source, outputs, count) | notebook doc | belongs in the .ipynb |
| Intents that must outlive a browser (execute, cancel, interrupt, restart) and kernel-owned live state (queue, stdin prompt, widget models, kernel_info) | K-doc (transient room) | must survive close/reopen, must not pollute .ipynb |
| Queries only the asker cares about (complete, inspect, history, is_complete), secrets (getpass reply), events (widget `custom` msgs, debugger) | unicast RPC on the room websocket (`RAW` message type already used for "save") | no broadcast, no CRDT history |
| Liveness / busy-idle | proxy awareness state | expiry = proxy-death detection for free |

Prototype measured completion through Y maps at ~10 ms median on localhost — fast enough — but it
broadcasts every request to all peers and records it in history, so RPC is preferred.

## 3. Execution design (recommended)

- Clients append `execute` intents to `K-doc.requests`: `{id, client_id, epoch, session_id, batch_id,
  cells: [{cell_id, source_hash}]}`. Run-all / run-below = one intent with an ordered cell list.
- The proxy alone orders work (arrival order at the server; no client clocks, no shared counter —
  pycrdt experiments showed `seq+1` counters lose increments and concurrent map sets pick an arbitrary winner).
  In one transaction it removes the intent and creates execution records (`queued → running →
  ok|error|aborted|cancelled|rejected|lost`).
- `epoch` bumps on every restart; stale intents (offline client, double-click restart) are rejected visibly.
- Proxy sends one execute_request at a time → cancellable queue, proxy-side stop-on-error per batch.
- Proxy writes `execution_count` and `execution_state=idle` in the same transaction (avoids a
  frontend-side idle write race).
- Recovery: `msg_id` + `running` written before send; restarted proxy follows iopub idle for that parent;
  running-with-idle-kernel → `lost`; never re-run (at-most-once).
- Compatibility: same shape as the REST proposal in jupyter_server#1736 / JEP pre-proposal #157
  (`{document_id, cells:[{cell_id, source_hash}], client_id, sequence}`) — a Y intent is just another
  front door to the same queue semantics.

Two variants:
- **Minimal**: no notebook schema change; `execution_state='running'` covers queued+running (as today).
  Additions: new `YKernelSession` doc type, reset stale `running` on load.
- **Clean**: add `'queued'` to `execution_state` (schema 2.0.0 → 2.1.0); kernel-owned outputs stored in a
  top-level map keyed by cell id (fixes the move problem below); real `move` in `moveCells`.

## 4. Biggest problems found (ranked)

1. **Cell moves destroy outputs.** JupyterLab's `moveCells` = delete + insert a clone from `toJSON`
   (`ynotebook.ts:252-263`). Proxy writes during a move land in a dead cell. Prototype works around it by
   keeping a shadow copy of running outputs and rewriting after a move (with a heuristic window). Two
   concurrent moves of the same cell produce **duplicate cell ids** (verified). Real fix: proper move, or
   outputs stored outside the cell map.
2. **Room lifetime.** jupyter-collaboration drops a room 60 s after the last websocket client leaves
   (`handlers.py:403-464`); in-process writers don't count. The kernel peer must pin the room or "close the
   browser and come back" loses outputs (nbmodel has this bug today).
3. **Frontend writes kernel-owned state everywhere** (CodeCell.execute, OutputArea, `onStateChanged`
   idle write on every client, runCells error path, set_next_input, metadata). The cell executor plugin
   must be replaced in any strategy; a fake kernel connection alone cannot prevent double writes.
4. **Y → view bugs in `@jupyterlab/cells`** (mid-list output inserts appended, one stream delta per
   transaction, local `\r` handling drift). Today they affect only collaborators; in this design every client.
   ~40 LOC upstream patch; proxy must do `\r`/`\b` processing.
5. **`ystate` and extra cell keys are wiped on reload** (`ynotebook.py:392`, `:631`) → kernel state needs
   its own doc/map.
6. **No cross-doc atomicity** (notebook doc vs K-doc) → brief inconsistent views; recovery code needed.
7. **pycrdt friction**: no origin on doc-level events, swallowed callback exceptions, nested-transaction
   errors when writing while iterating, task-affine connections, ints come back as floats.

## 5. Volume

Prototype, 1000 `\r` progress updates on one line:

| | Y updates | bytes on wire | final doc growth |
|---|---|---|---|
| no throttle | 1004 | 68 KB | +248 B |
| flush ≤ every 50 ms | 57 | 3.9 KB | +248 B |
| flush ≤ every 100 ms | 31 | 2.1 KB | +248 B |
| no `\r` handling in proxy | — | — | +33 KB, unbounded |

GC keeps the in-memory doc small; the cost is wire traffic and the YStore update log (never compacted by
default, `stores.py:30-36`). With the server as single source of truth, the proxy can throttle, coalesce,
and the store can be compacted/snapshotted aggressively. jupyter-server-documents' "outputs on disk + URL
placeholder" is another option for large outputs.

## 6. Side channels (summary of recommendations)

- **stdin**: prompt in K-doc (all collaborators see it, survives reload); reply only over RPC, never in Y
  (getpass); first valid reply wins; masked echo in outputs. Policy options: only requester answers until
  they disconnect; `allow_stdin=False` when nobody connected; timeout.
- **Widgets**: proxy is the kernel's only widget client; mirrors models per attribute into K-doc (+ binary
  buffers); frontends use a Y-backed comm shim so existing JS widget libraries keep working. Output widget
  becomes reliable (proxy routes by msg_id even with no browser). Play/jslink/custom JS-computed state still
  need a browser. Long-term: Y-native widgets (ypywidgets style).
- **Debugger**: tunnel DAP over RPC; a direct websocket only while debugging is an acceptable phase-1.
- **display_id / update_display_data**: proxy keeps an index to output Y objects (works across cells and
  reload — today JupyterLab loses it on reload).
- **Consoles**: later, a console Y doc reusing the same intents.

## 7. Frontend plan (no UI change, no fork)

- **Phase 1 (~940 LOC)**: replace `INotebookCellExecutor`; stdin renderer, status bridge, trust sync,
  dev-mode guard against stray writes. Keep kernel websocket for completer/tooltip/comms/debugger/console;
  server filters output IOPub on that websocket (hook likely `kernel_websocket_connection_class`, unverified).
- **Phase 2 (~1,000–1,300 LOC)**: custom `IKernelManager` (JL 4.4+) returning a Y-backed `YKernelConnection`;
  real websocket opened lazily only for debugger/console/extensions calling `requestExecute`.

## 8. Divergences between the design notes and the prototype

The prototype was built in parallel and made simpler choices; worth noting:
- Ordering: prototype uses `seq = max_seen + 1` with client-id tiebreak; design recommends append-only
  intents ordered by the proxy (the counter approach can produce surprising interleavings).
- Source snapshot: prototype reads source when the proxy picks the cell; design recommends snapshot (or
  source hash check) at queue time, matching today's behaviour and the REST proposal.
- Completion: prototype uses a Y map RPC (works, ~10 ms); design recommends out-of-doc RPC.

## 9. Open questions for decision

Execution
1. On error/interrupt: abort only the failing batch, or the whole kernel queue as today?
2. One execute_request at a time (cancellable) vs pipelining to the kernel?
3. Same cell queued twice: run twice or coalesce?
4. Clear outputs at queue time (today) or at start?
5. Schema: minimal (no notebook change) or clean (minor bump, `queued` state, outputs map keyed by cell id)?
6. One K-doc per kernel (proposed; shared by notebooks/consoles on that kernel) or per notebook?
7. How to show a "lost" execution after a proxy crash?

Architecture
8. Proxy in-process in jupyter-server (simpler: RPC = new RAW subtype, room pinning) or separate process?
9. Is a non-persisted K-doc that lives as long as the kernel session acceptable?
10. Engage with jupyter_server#1736 / JEP #157 authors, positioning Y intents as an alternate front door?

Side channels
11. stdin: who may answer; refuse input when nobody is connected?
12. Hide busy/idle caused by completions from the status indicator?
13. May the proxy apply `set_next_input` (edit source / insert cells) — the one non-kernel-owned write?
14. Widgets: comm-shim short term, Y-native long term? Emulate Play/jslink server-side?
15. Debugger: direct websocket acceptable for phase 1?
16. Upstream fixes to pursue first: real `moveCells`, Y→view bugs in `@jupyterlab/cells`, room pinning?
