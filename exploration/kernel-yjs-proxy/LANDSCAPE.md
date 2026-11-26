# Landscape: moving the kernel→document translator to the server

Talking points for discussion. Details and citations: `DESIGN.md`, `notes/01..04`, `prototype/`.

## 1. Thesis

Today **the browser tab that ran a cell is the translator** between the Jupyter protocol (ZMQ, via the
kernel websocket) and the shared Y document. It holds the in-memory state needed to do that
(futures keyed by `msg_id`, the `display_id` map, pending `clear_output(wait=True)`, terminal
`\r`/`\b` processing, stream merging) and writes the results into the Y doc as one collaborator among
others.

Any server-side execution effort (jupyter_server#1736 / JEP #157, jupyter-server-nbmodel,
jupyter-server-documents, jupyverse) has to **re-implement that translator on the server**. Since it is
being moved anyway, we get to choose its shape — and the cheapest moment to also make "kernel + translator"
a plain Y collaborator, and to fix the document model where it fights this, is now.

## 2. What the browser translator does today (all of it must move)

| Responsibility | Where today (JupyterLab) | Problem with it living in one browser |
|---|---|---|
| Route iopub by parent `msg_id` → cell | `OutputArea._onIOPub`, futures (`outputarea/src/widget.ts:830-880`) | Tab closes → outputs lost |
| Merge consecutive streams, apply `\r`/`\b` | `OutputAreaModel._add` (`outputarea/src/model.ts:340-395`) — on the *reader* side | Each client post-processes differently → views drift from Y |
| `display_id` → outputs index for `update_display_data` (cross-cell) | kernel connection, frontend only | Lost on reload; collaborators can't apply it |
| `clear_output(wait=True)` pending state | OutputArea | Per-tab state |
| `execution_count`, `execution_state` (`running` at schedule, `idle` reaction on *every* client) | `cells/src/widget.ts:1829, 1879, 1666-1669` | Last-writer-wins races; stuck `[*]` |
| Timing metadata (`record_timing`) | `cells/src/widget.ts:1845-1909` | Per-tab |
| Payloads: `set_next_input`, pager | `notebook/src/cellexecutor.ts:156-203`, `outputarea/src/widget.ts:882-922` | Per-tab |
| stdin prompt + echo into outputs | `outputarea/src/widget.ts:615-628` | Only the executing tab can answer; lost on reload |
| Queue: send all `execute_request`s at once, kernel queues; abort-on-error handling | `notebook/src/actions.tsx:3365, 3420-3430` | Queue invisible to collaborators |
| Widget comms; Output widget captures by `msg_id`; state saved to metadata | jupyterlab_widgets manager (`manager.ts:446-466`) | Widget state dies with the tab |
| `language_info` / `kernelspec` metadata | `notebook/src/panel.ts:224-290` | Written by every client |

## 3. Three independent axes (the landscape is clearer once separated)

- **A. Where the translator lives**: browser | server.
- **B. How execution intents enter**: kernel websocket messages | REST endpoint | Y intents in a (side) doc.
- **C. How non-execution traffic flows** (complete, inspect, stdin replies, comms, debugger, status):
  kernel websocket | unicast RPC on the room socket | Y doc / awareness.

| Project | A translator | B intents | C side traffic | Notes |
|---|---|---|---|---|
| JupyterLab RTC today | browser | kernel ws | kernel ws | outputs die with the tab |
| jupyter-server-nbmodel | server | REST (+ code in body) | kernel ws | one-at-a-time worker; stdin REST-polled; no `update_display_data`; room not pinned |
| jupyter-server-documents | server (room owns kernel client) | REST, source-hash checked batches | kernel ws; stdin disabled | outputs-on-disk + URL placeholders |
| jupyverse | server | REST `{document_id, cell_id}` | kernel ws; ypywidgets | no stdin |
| jupyter_server#1736 / JEP #157 | server | REST `{document_id, cells:[{cell_id, source_hash}], client_id, sequence}` | unspecified | silent on status/stdin/widgets |
| **This exploration** | server, as a Y peer | Y intents in a per-kernel side doc | RPC on room socket; status in awareness; stdin prompt + widget state in side doc | prototype: 26 tests passing |

**Key point:** the hard, shared part is axis A — the translator's semantics. Axis B is a thin front door
once A exists: a Y intent and a #1736 REST call can feed *the same queue* with the same
`{cell_id, source_hash, client_id, sequence}` semantics. Suggest agreeing on the translator core first
and treating front doors as pluggable.

## 4. Issues you hit by moving the translator server-side — regardless of front door

1. **`moveCells` = delete + insert a clone** (`jupyter_ydoc` `ynotebook.ts:252-263`). Outputs written
   during a move go to a dead cell; concurrent moves duplicate cell ids (verified). → real move op, or
   kernel-owned outputs stored outside the cell map, keyed by cell id.
2. **Room lifetime**: room dropped 60 s after last websocket client (`jupyter-collaboration`
   `handlers.py:403-464`); in-process writers don't count. → translator must pin the room.
3. **Frontend still writes kernel-owned state** (table above) → replace `INotebookCellExecutor`
   everywhere, neutralise the `onStateChanged` idle write (write count + idle in one transaction),
   dev-mode guard.
4. **Y→view reader bugs** in `@jupyterlab/cells` (`model.ts:860-904`, `ycell.ts:948-975`): mid-list
   inserts appended, one stream delta per transaction, reader-side `\r` drift. Today only collaborators
   hit them; after the move, everyone. ~40 LOC upstream fix; translator does `\r`/`\b` itself.
5. **`ystate` and extra cell keys wiped on reload** (`ynotebook.py:392, 631`) → kernel state needs its
   own doc or root map.
6. **`display_id` index** must live server-side (and ideally survive translator restart).
7. **Translator needs its own ZMQ session id** (ROUTER_HANDOVER steals replies otherwise; nbmodel does this).
8. **Volume**: translator throttling makes it cheap (1000 progress updates: 1004 → 31 Y updates at
   100 ms flush; 68 KB → 2 KB on the wire). YStore never compacts by default → server-as-truth allows
   aggressive compaction.
9. **stdin / widgets / Output widget**: now possible to make durable (prompt survives reload; Output
   widget routed by `msg_id` with no browser open), but needs a design (see `notes/03`).

## 5. What the Y-intent front door adds on top

Buys:
- One channel; kernel + translator is literally "another collaborator".
- Queue/intent state visible to every collaborator (queued cells show everywhere, not just in the
  requesting tab).
- Intents survive the requesting browser; no extra REST/session plumbing.

Costs / needs:
- Intents live in CRDT history → need a restart `epoch` and expiry so stale offline intents are rejected.
- Ordering must be decided by the translator (shared counters lose increments under merge — verified).
- No atomicity across notebook doc and side doc.
- Completion/inspect should *not* go through Y (broadcast + history), even though it is fast enough
  (~10 ms localhost in the prototype) → unicast RPC.

## 6. Proposed things to settle in the discussion

1. Agree that the translator moves server-side and is specified once (queue semantics, state machine,
   display index, terminal processing, stdin, room pinning), independent of front door.
2. Document-model fixes worth doing at the same time: real `move`; `queued` execution state; possibly
   kernel-owned outputs keyed by cell id outside the cell map.
3. Is the translator a Y peer (in-process in jupyter-server first, separable later)?
4. Front doors: #1736 REST and Y intents feeding the same queue?
5. Who owns the upstream JupyterLab fixes (executor swap, reader bugs, idle-write race, trust sync)?
