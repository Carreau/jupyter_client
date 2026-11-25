# 02 — Execution semantics and document schema

Topic: how "execute this cell" (and interrupt / restart / run-all / cancel) is expressed as CRDT state,
how the kernel proxy claims and reports work, and what the notebook + a side "kernel session"
document look like.

Claims marked **[verified]** were checked with pycrdt 0.14.13 (the yrs engine, same algorithm as Yjs) in
`/tmp/claude-0/-home-user/1d2380c5-8349-57e4-9bdb-3b7154acc60b/scratchpad/exec-design/merge_experiments.py`
(E1–E11) and `.../merge_experiments2.py` (F1–F6). Re-run with `/home/user/venv/bin/python <file>`.

---

## 0. Where we start (current schema and behaviour)

- Cell schema (`jupyter_ydoc/ynotebook.py:36-55`): `cells: YArray[YMap{id, cell_type, source: YText,
  metadata: YMap, execution_state: str, execution_count, outputs: YArray, attachments}]`, plus root
  `meta` and `state` maps. Document version is `"2.0.0"` (`ynotebook.py:81`).
- `execution_state` already exists. It is typed `'running' | 'idle'` (`javascript/src/api.ts:537`),
  set to `"idle"` when a code cell is created (`ynotebook.py:200`), and **removed when the notebook is
  saved** (`ynotebook.py:120`). JupyterLab sets it to `'running'` **when the cell is queued**, not when it
  starts. Its own comment says *"in future we would like to distinguish running from scheduled"*
  (`jupyterlab/packages/cells/src/widget.ts:1828-1829`).
- The model fixes three things that limit any "put the request on the cell" design:
  1. **Moving a cell deletes it and inserts a clone.** JS `moveCells` copies `toJSON()` into a brand-new
     Y.Map (`javascript/src/ynotebook.ts:252-263`). `toJSON` keeps only the nbformat fields
     (`ycell.ts:934-938`), so `execution_state` and any extra key are lost. The Python `set()` also
     moves cells by delete plus recreate (`ynotebook.py:464-467`). pycrdt 0.14's `Array` has no `move`.
  2. **Extra cell keys get saved to the .ipynb.** Only `execution_state` is popped on save **[verified F4]**.
  3. **Extra cell keys are deleted when the file is reloaded from disk.** `_update_cell` deletes the
     `removed_keys` (`ynotebook.py:631-632`). `execution_state` survives only because it was popped
     from `old_cell` first **[verified F5]**. Every key in `ystate` except `dirty/path/document_id` is
     wiped by `set()` (`ynotebook.py:392-393`) **[verified F6]**.
- **An extra top-level root type (e.g. `ydoc.get("execution", type=Map)`) is ignored by `YNotebook.get()`
  and survives `set()`** **[verified F3]**. This is the cheapest way to add state to the notebook doc.
- Prior art: jupyter-server-nbmodel already runs cells on the server and writes into the Y cell. It sets
  `execution_state="running"`, clears outputs and sets `execution_count=None` at **start**
  (`jupyter_server_nbmodel/actions.py:465-472`), then `execution_count` and `idle` on reply (`:541-542`).
  Its queue is a server-side in-memory `asyncio.Queue` per kernel (`execution_stack.py:92,562`). It
  needed `dedup_task_queue` because "restart and run all" left stale tasks (`actions.py:584-610`). That
  is exactly the problem an epoch solves (section 4).
- Kernel abort semantics (ipykernel): `stop_on_error` defaults to True (`kernelbase.py:817`). After an
  error reply the kernel enters "aborting" mode (`:881-883`, `_abort_queues` `:1320`). Requests already
  queued, or arriving within `stop_on_error_timeout` (default 0.0, `:265`), get an `execute_reply` with
  `status: "aborted"` (`:1361-1370`).

## 1. CRDT facts that drive the design (all verified)

| # | Fact | Consequence |
|---|---|---|
| E1 | Concurrent `map[k] = v` on the same key: exactly one value wins, the same on all peers. Winner = higher client id (pycrdt/yrs). | Map keys work as *idempotent* intents ("cell X should run"). They cannot count, and the winner is arbitrary, not the latest by time. |
| E2 | Read-modify-write `seq = seq + 1` from two peers gives `seq+1`: **one increment is lost**. | No client-maintained global counter. A per-client G-counter `{client_id: n}` does work (F1). |
| E3 | Concurrent appends at the same position converge. Each peer's run stays **contiguous**, and the lower client id goes first. | A Y.Array queue gives a total order that all peers agree on. Across peers the order is arbitrary, not by wall clock. |
| E4 | A batch appended in one call (`extend`) stays contiguous under a concurrent append. | "Run all" written as one batch cannot be interleaved by another user's request. |
| E5 | A cell moved (delete plus clone) while the proxy writes outputs/state into the old Y.Map: the proxy's writes are **lost**. The clone has no outputs and no `execution_state`. | The proxy must look the cell up by `id` on every write, and must be able to re-write outputs after a move. Per-cell request keys are lost on move. |
| E6 | A write into a concurrently deleted cell does not bring the cell back. | Safe: a deleted cell stays deleted. The proxy just loses those outputs. |
| E7/E11 | Replacing a whole nested Map concurrently with edits inside the old one: the edits inside are lost. | Never let two parties write the same container. Split keys by owner (E8: disjoint keys on one map never conflict). |
| E9 | Deletions are permanent. An offline client that reconnects re-adds only its *new* items, not items others deleted. | The proxy can compact the request log by deleting consumed entries. An offline client's new intents do arrive late, so they need an epoch / staleness check. |
| E10, F2 | `observe` / `observe_deep` fire for remote updates applied via `apply_update`. `observe_deep` on `cells` gives `path=[index]` and `keys` for a per-cell key write. | The proxy can find new work with observers alone, no polling. |

One more failure mode, inferred from how Yjs sync works and not tested: if the side doc lives in a
*transient* (in-memory) room and the server restarts, a client that kept its old copy will **push the
old content into the new empty room** when it reconnects. Every intent therefore needs an epoch /
session id that the proxy checks. It cannot assume an item is new just because the item is present.

## 2. How a client says "execute this cell": alternatives

Every design below shares one rule: **clients write intents; only the proxy writes kernel-owned state**
(`execution_state`, `execution_count`, `outputs`, execution records). Ownership is split by key, so
client and proxy never write the same key (E7/E8).

### A. Per-cell `execution_request` map in the notebook cell, plus a global counter

```ts
cell.execution_request = { rid: uuid, seq: number, client_id: number, ts: number, source?: string }
cell.execution_ack     = rid            // proxy-owned
```
- *Two clients queue at once:* the request is idempotent per cell, so one rid wins (E1) and the cell
  runs once. That is acceptable. Ordering *across* cells needs `seq`, and a shared counter loses
  increments (E2). So `seq` must be `(ts, client_id)`, i.e. option C in disguise.
- *Same cell queued twice:* the second request overwrites the first. If the first was already claimed,
  `rid != execution_ack` means "run again". If it was not, the two coalesce. "Run twice" cannot be
  expressed except with a G-counter.
- *Cell deleted while queued:* the request goes with the cell. Fine.
- *Cell moved while queued:* **the request is lost** (E5, `ycell.ts:934`), unless `toJSON` and the
  Python `set()` move path are taught to copy it. A cell being moved by a collaborator while queued is
  routine during a long run-all.
- *Cell edited while queued:* see section 3.
- *Detection:* `observe_deep` on `cells`, filtering for the `execution_request` key (F2). Cheap.
- *Run all:* N writes in one transaction with `seq = (ts, client, i)`. The proxy sorts its pending set
  by `seq`.
- *Schema cost:* the new key is saved to the .ipynb (F4) and deleted on reload from disk (F5), so
  `_cell_to_py`, `_update_cell` and JS `toJSON`/`createCell` must all change. Old frontends would also
  drop the key on move. **Not purely additive.**

### B. Append-only Y.Array intent log in a side "kernel session" doc; the proxy sets the order (recommended)

```ts
session.requests: Y.Array<Intent>   // clients append; only the proxy deletes (after consuming)
session.queue:    Y.Array<ExecId>   // proxy-only: the order it will run in
session.executions: Y.Map<ExecId, ExecRecord>  // proxy-only
```
- *Two clients queue at once:* both intents arrive, and the array order converges (E3). The **proxy**
  then fixes the real order: it appends to `queue` in the order it *observes* intents, which is arrival
  order at the server. Clients never need a counter or clock, and there is one writer, so no lost
  updates.
- *Same cell queued twice:* two intents produce two ExecRecords and the cell runs twice, the same as
  today's JupyterLab, where each Shift-Enter sends one execute_request. The proxy may coalesce instead
  if we decide to (open question Q4).
- *Offline client with stale requests:* intents carry `epoch` (kernel incarnation) and `session_id`
  (room incarnation). The proxy rejects mismatches with `state: 'rejected', reason: 'stale'`, so the
  client can show "your queued run was dropped". Optionally it also applies a TTL on `ts` (Q3).
- *Cell deleted while queued:* when the proxy is about to start, it looks up `cell_id` in the target
  doc. If the cell is missing, the record becomes `cancelled/cell_deleted`. This is better than today,
  where the request is already in the kernel and runs anyway with its outputs thrown away.
- *Cell moved while queued:* no effect. Records point at `cell_id`, never at a Y.Map.
- *Cell edited while queued:* see section 3.
- *Detection:* `requests.observe()` on inserts (E10). On startup, the proxy scans `requests` for
  entries not yet in `executions`.
- *Claim / ack:* in **one transaction** the proxy deletes the intent from `requests`, creates its
  ExecRecords with `state: 'queued'`, appends them to `queue`, and mirrors `execution_state` into the
  notebook cell. Nothing is ever ambiguous: an intent is either still in `requests` (unclaimed) or has
  records (claimed). The two cannot both be true for an observer, because a transaction is atomic per
  update. Deletion is idempotent, and a deleted item stays deleted (E9).
- *Run all / run selected and below:* one intent with an ordered `cells: [id…]` list, taken at click
  time. It turns into contiguous records, appended to `queue` together. Document order at click time is
  preserved by construction (E4 covers even the raw log).
- *Schema cost:* the notebook needs nothing new (minimal variant). Everything else lives in a new doc type.

### C. Per-cell Lamport/HLC timestamp

```ts
cell.execution_request = { hlc: [physical_ms, logical, client_id], rid }  // in cells, or in a root map keyed by cell id
```
- An HLC fixes A's ordering problem without a counter. Ties break by client id, so the order is total
  and deterministic. Each client's own requests keep causal order (HLC is at least as large as anything
  it has seen).
- It still has A's coalescing semantics, and A's move problem unless it lives in a root map (see D).
- Ordering uses client clocks. A skewed client can jump the queue, and an HLC only limits skew relative
  to messages the client has *seen*. Nobody is malicious here (trust: good faith), but an offline client
  coming back with a far-future HLC would still sort first. That is worse than B, where the proxy orders
  by arrival.
- Useful when no single process orders the work, e.g. several proxies or a P2P setting. We have a single
  proxy, so the extra machinery buys nothing.

### D. Root-level `execution` map in the notebook doc, keyed by cell id

```ts
nbdoc.get("execution", Map): { [cell_id]: { request: {rid, hlc, epoch, client}, /* client-owned */
                                            claim:   {rid, state, exec_count}   /* proxy-owned */ } }
```
- Survives moves, invisible to save, survives `set()` **[verified F3]**. Old clients simply ignore it.
- It is persisted in the notebook's ystore (`.{file_type}:{file_id}.y`, `jupyter_server_ydoc/handlers.py:131`)
  together with the notebook, so after a server restart stale requests are still there. The epoch
  filter handles that, but the stale requests clutter the document.
- It is notebook-local: a console, or a second notebook on the same kernel, cannot share one queue
  without a side doc anyway.
- Good *fallback* if adding a second room per notebook is rejected. Then use HLC ordering (C), or put
  the `requests` log of B in this root instead of a side doc.

### Comparison

| | A cell key + counter | **B log + proxy queue** | C HLC | D root map |
|---|---|---|---|---|
| Lost increments | yes (E2) | n/a | n/a | n/a |
| Survives cell move | **no** (E5) | yes | only in root map | yes |
| Run twice | no (coalesces) | yes (parity) | no | no |
| Order source | broken / clocks | **server arrival** | client clocks | client clocks |
| Shared kernel (console, 2 notebooks) | no | **yes** | no | no |
| Notebook schema change | key + strip + move | **none** | key + strip + move | additive root |
| Stale-intent protection | epoch field | epoch + session id | epoch | epoch |
| Unambiguous claim | ack key | **intent consumed + record, one txn** | ack key | claim sub-key |

**Recommendation: B.** Intents go in a log in the kernel-session doc. The proxy turns them into an
ordered, proxy-owned queue of execution records and *mirrors* per-cell state into the notebook.

## 3. Source snapshot: queue time or start time?

| | Queue time (snapshot in the intent) | Start time (read the Y.Text when dispatched) |
|---|---|---|
| Today's behaviour | **Same.** JupyterLab sends the execute_request immediately, so the kernel has the code at click time. nbmodel also takes the code in the request body. | Different |
| Edit-while-queued | Runs what the user saw when they pressed Run. Later typing (theirs or a collaborator's) does not change what runs. | Runs whatever text is there when its turn comes, possibly half-typed by someone else. Surprising and racy. |
| Cost | Source is duplicated into the session doc, which is transient and trimmed after completion. Big cells cost bytes, but a deleted string's content is dropped by GC. | Free |
| Offline / stale | A late-arriving intent carries exactly the code that was meant. | Would run current text that the requester never saw. |
| Cell deleted while queued | Could still run from the snapshot. We still choose to cancel (section 2B). | Must cancel |

**Recommend queue time.** The executing client writes `sources[i]` together with `cells[i]` in the
intent. Consequences:
- The displayed source can differ from what produced the outputs. The proxy stores the source hash in
  the record (`source_sha`), so a frontend can mark the cell "edited since run" (that is UI, optional).
- The proxy must **not** re-read the cell text. For a *headless* trigger such as a REST "run all"
  without a browser, the proxy fills in sources from the doc at the moment it claims the intent (same
  result).
- Memory: records keep `source` only while `queued`/`running`. On completion the field is deleted and
  the hash kept.

## 4. Per-cell state machine and field ownership

```
              client intent            proxy dispatch          execute_reply
   idle ───────────────────▶ queued ──────────────▶ running ──────────────────▶ idle
     ▲                         │  │                    │   status=ok    → record.state='ok'
     │  cancel intent          │  │ stop_on_error /    │   status=error → 'error'  (abort rest, below)
     └─────────────────────────┘  │ interrupt / restart│   status=aborted → 'aborted'
         record 'cancelled'       ▼                    │   kernel died / restart → 'lost' | 'error'
                           'aborted' / 'cancelled'     ▼
                           'rejected' (stale epoch)  (proxy crash: see section 6)
```

Two layers:
- **ExecRecord.state** (session doc, proxy-owned): `queued | running | ok | error | aborted | cancelled |
  rejected | lost`. Kept in full, so the UI and tooling can tell queued from running and why something
  did not run.
- **Notebook cell mirror** (proxy-owned, existing keys only):

| Field | Writer | When |
|---|---|---|
| `execution_state` | proxy | `'running'` when **queued** (minimal variant, the same meaning as today's `[*]`), or `'queued'` then `'running'` (clean variant, section 7). Back to `'idle'` on any terminal state. |
| `outputs` | proxy | Cleared at **start** (as nbmodel does at `actions.py:470`), not at queue time as today (`cells/src/widget.ts:1822`). A cancelled cell therefore keeps its previous outputs. (Q5) |
| `execution_count` | proxy | `None` at start. Set from the `execute_input` iopub message (it carries `execution_count`) or, failing that, from `execute_reply`. **Only the proxy writes it.** Clients never touch it. |
| `metadata.execution.*` timing | proxy | Same keys as jupyterlab-execute-time / nbmodel (`actions.py:483-487`), only if `record_timing`. |
| `source`, `metadata` (other) | clients | unchanged |

**Parent msg_id ↔ cell.** The proxy creates `msg_id` itself and stores it in the record
(`record.msg_id`) *before* sending. It keeps an in-memory `msg_id → exec_id` index, rebuilt from
`executions` on restart. For each iopub message: `parent_header.msg_id → record → (doc, cell_id) →`
**look up the Y cell by id at write time**, never a cached Y.Map (E5). The proxy also keeps the running
cell's output list in memory. When it sees the target cell's Y.Map deleted and an equal `id` reappear
(a move), it rewrites the full outputs into the new Y.Map in one transaction. That repairs E5. Deleted
for good means outputs are dropped and the record still finishes. Duplicate cell ids, which the
dedup at `ynotebook.py:288-300` tolerates, resolve to the first match.

**execute_reply status=error, and stop_on_error.** The proxy dispatches **one execute_request at a time
(window = 1)** by default, so the kernel's shell queue holds at most one of our requests:
- On `status: error` with the batch's `stop_on_error=true`, the proxy marks every remaining `queued`
  record **of the same batch** as `aborted` (reason `previous_error`, `aborted_by: exec_id`) and sets
  their cells back to `idle` with outputs unchanged. Other users' batches keep going (Q1: today's
  kernel-side behaviour aborts *everything* queued, which in a shared notebook means Alice's typo cancels
  Bob's run).
- If pipelining is enabled (window > 1, a latency optimisation for many tiny cells), the kernel itself
  answers `status: aborted` for the in-flight requests (`kernelbase.py:1361-1370`). The proxy records
  `aborted` for those and applies the same rule to the not-yet-sent remainder. Window = 1 keeps cancel
  and reordering simple, and keeps the abort scope under the proxy's control. The cost is one shell
  round trip per cell (sub-millisecond locally).
- `silent` / `store_history=false` executions (used by widgets and helper code) do not abort user
  batches.

## 5. The kernel-session document

### Room and linking
- Room id `kernel-session:<kernel_id>`. With a single `:` it is a **TransientRoom**: in memory, no
  ystore (`jupyter_server_ydoc/handlers.py:115,153-158`). Its lifetime matches the kernel's, which is
  also in memory. The proxy is a server-side client of that room *and* of every notebook room it serves.
- **One session doc per kernel**, not per notebook. Today a notebook and a console can share a kernel
  through the sessions API. Both then append to the same `requests` and see the same status, queue and
  `execution_count`.
- Notebook ↔ kernel link: discovered through the existing sessions API (`path → kernel_id`), so there is
  no UI change. The session doc lists `attached` documents, so the proxy knows which notebook rooms to
  join. We do **not** store the link in the notebook's `ystate`: it is wiped by `set()` (F6), and
  persisting a kernel id in the file is wrong anyway.
- **Several notebooks on one kernel:** one FIFO across documents (the kernel runs one thing at a time
  anyway). `execution_count` is per kernel, so it jumps between notebooks, which is already the case
  today. Records carry `doc` (room id), so outputs go to the right place. Stop-on-error stays per
  batch, so notebook 1's error does not abort notebook 2's run.

### Schema (pycrdt-ish)

```python
# room "kernel-session:<kernel_id>"
class YKernelSession(YBaseDoc):        # version "1.0.0"
    state: Map                          # YBaseDoc standard
    kernel: Map = {                     # PROXY-ONLY
        "id": str, "name": str,         # kernelspec name
        "spec": {"display_name": str, "language": str, "metadata": {...}},
        "status": "starting"|"idle"|"busy"|"restarting"|"autorestarting"|"dead"|"unknown",
        "epoch": int,                   # +1 on every (re)start; intents must match
        "session_id": str,              # uuid per room incarnation (defeats the stale-room push, section 1)
        "current": str | None,          # exec_id that is running
        "execution_count": int,         # last seen
        "kernel_info": Map,             # kernel_info_reply content incl. language_info, help_links, banner
        "last_error": str | None,       # e.g. "kernel died (exit code 137)"
    }
    attached: Map[str, Map]             # PROXY-ONLY  doc room id -> {"kind": "notebook"|"console", "since": ts}
    requests: Array[Map]                # CLIENTS APPEND, PROXY DELETES
    queue: Array[str]                   # PROXY-ONLY  exec ids: running first, then queued, in run order
    executions: Map[str, Map]           # PROXY-ONLY  exec_id -> ExecRecord; terminal ones trimmed to last N / T
```
```ts
type IntentBase = { rid: string; client: number; user?: string; ts: number; epoch: number; session_id: string };
type ExecuteIntent   = IntentBase & { kind: 'execute'; doc: string; cells: string[]; sources: string[];
                                      stop_on_error: boolean; record_timing?: boolean; silent?: boolean;
                                      store_history?: boolean; position?: 'end' | 'front' };
type CancelIntent    = IntentBase & { kind: 'cancel'; exec_ids?: string[]; batch?: string; doc?: string; cells?: string[] };
type InterruptIntent = IntentBase & { kind: 'interrupt'; target: string | null };   // exec_id the user saw running
type RestartIntent   = IntentBase & { kind: 'restart'; then?: Omit<ExecuteIntent, keyof IntentBase> };
type ShutdownIntent  = IntentBase & { kind: 'shutdown' };
type ChangeKernelIntent = IntentBase & { kind: 'change_kernel'; name: string };    // maybe; or keep REST

type ExecRecord = {
  id: string;            // `${batch_rid}:${i}`, so it is deterministic and survives proxy restart
  batch: string; doc: string; cell_id: string;
  source?: string; source_sha: string;
  state: 'queued'|'running'|'ok'|'error'|'aborted'|'cancelled'|'rejected'|'lost';
  reason?: 'previous_error'|'interrupted'|'restart'|'kernel_died'|'cell_deleted'|'stale'|'user'|'proxy_restart';
  msg_id?: string; epoch: number; execution_count?: number;
  requested_by: number; queued_at: number; started_at?: number; ended_at?: number;
};
```
Status changes and similar fast-moving values: `kernel.status` changes on every execution
(busy/idle), producing small updates. That is acceptable. If the churn matters, it could move to the
session room's **awareness** (`pycrdt/_awareness.py:111`), which is also the right place for proxy
liveness (`{role: "kernel-proxy", epoch, pid}`). That avoids a persistent heartbeat key; the status is
recoverable from the kernel anyway.

## 6. Interrupt, restart, run all, cancel as intents

All intents are **targeted**, so a late arrival is harmless:
- **Interrupt** `{target: exec_id}`. The proxy acts only if `kernel.current == target`
  (or `target == null` and something is running and the epoch matches). An offline client's
  "interrupt" from ten minutes ago is ignored instead of killing whatever runs now. Effect: the proxy
  calls the kernel manager's interrupt (signal, or `interrupt_request` on control). The running record
  ends `error` (KeyboardInterrupt) with reason `interrupted`. Then **today's semantics**: everything
  queued is `aborted`, because the interrupted user nearly always wants the run-all stopped. Q1 asks
  whether that should be scoped to the interrupter's batch or the whole queue.
- **Restart** `{epoch: e}`. Ignored unless `e == kernel.epoch`, so two clients clicking restart at
  once give **one** restart (both target epoch `e`, the proxy does the first and the second goes stale).
  Effect, in one transaction: `epoch += 1`, `status = restarting`, all `queued` records go to
  `cancelled/restart`, the running one goes to `lost/restart` (or `error`), and the cells are mirrored
  back to idle. Any intent still sitting in `requests` with the old epoch is `rejected/stale`.
- **Restart and run all** = `restart` with `then: {cells, sources, …}`. The follow-up is embedded, so
  it is enqueued under the *new* epoch atomically. This is the case nbmodel had to patch with
  `dedup_task_queue` (`actions.py:584`).
- **Run all / run selected and below / run above**: these are plain `execute` with an explicit ordered
  cell list, built by the client from the document order at click time. Cells inserted afterwards do
  not run, matching today.
- **Cancel queued cells** (new capability; today you can only interrupt): `cancel` by exec id, batch,
  or `(doc, cells)`. `queued` → `cancelled/user`. Records already `running` are left alone (cancelling
  those means interrupt). This is cheap *because* of window = 1: nothing queued has reached the kernel.
- **Shutdown**: `status = dead`, every non-terminal record goes to `cancelled`, and the room stays (or is
  dropped) so clients see the final state.
- "Clear outputs" / "clear all outputs" stays a plain client edit of `outputs` for idle cells, as today.
  If a cell is `running`, the client sends a `clear_outputs` intent instead, so it does not race the
  proxy's writes (the E7 single-writer rule).

## 7. Proxy crash or restart: recovery and idempotency

Invariants that make recovery mechanical:
1. Every ExecRecord transition is a single proxy transaction, and `msg_id` + `state='running'` are
   written **before** the ZMQ send.
2. Exec ids are deterministic (`batch:i`). Re-processing an intent whose records already exist is a
   no-op, which makes claiming idempotent.
3. A record is terminal or non-terminal. Only `queued` records are ever dispatched.

On start (same server process, kernel still alive, session room still in memory):
- Rebuild `msg_id → exec_id` from `executions`. Re-scan `requests` for unclaimed intents (process those
  whose epoch and session_id match, reject the rest).
- `kernel_info_request` / status probe. Subscribe to iopub **before** deciding anything.
- A `running` record whose kernel is still busy on its msg_id keeps going: iopub messages with
  `parent_header.msg_id == record.msg_id` keep flowing and are appended. The `execute_reply` went to the
  dead proxy's shell socket and is lost. **Completion = iopub `status: idle` with that parent.** The
  status is then `ok` unless an `error` output was seen. `execution_count` comes from `execute_input`,
  if we caught it, or stays None.
- A `running` record while the kernel is idle (or reports another msg) is `lost/proxy_restart`, and the
  cell goes back to idle with an explanatory output (Q6). **At most once:** we never re-send, because
  execution has side effects. The window "state=running written, then crash before send" is treated
  the same way. It is ambiguous, and at-most-once is the safe choice.
- `queued` records: continue dispatching in `queue` order (epoch unchanged, so they are still valid).

On a server restart (kernels die, transient session room gone, notebook restored from ystore):
- The notebook doc may contain cells with `execution_state='running'`, persisted in the ystore. When
  the proxy (or the room loader, if no kernel is attached) attaches to a notebook, it resets every cell
  whose `execution_state != 'idle'` and that has **no non-terminal record in the current session**.
  This should also be done in jupyter-collaboration's DocumentRoom load path for notebooks without a
  kernel.
- A client reconnecting with an old copy of the session doc pushes stale items into the fresh room
  (section 1). `session_id` and `epoch` reject them. The client should also discard its local copy when
  `kernel.session_id` differs from the one it cached. The frontend YKernelSession model drops and
  recreates its Y.Doc in that case.

On kernel death (autorestart): the same transaction as restart, with reason `kernel_died` and
`kernel.last_error` set.

## 8. Minimal variant vs clean variant

### Minimal (purely additive; no notebook schema version bump)
- Notebook doc: **no new keys.** The proxy writes the existing `execution_state` (`'running'` for
  queued and running, exactly today's meaning per `cells/src/widget.ts:1829`), `execution_count`,
  `outputs` and `metadata.execution`. Old frontends render `[*]` and outputs correctly with no change.
- New `YKernelSession` class in jupyter_ydoc (Python + JS) registered as a new doc type, and the
  `kernel-session:` room in jupyter-collaboration. Additive.
- The frontend fine-grained state (queued vs running, why cancelled) comes from the session doc.
- Small, non-schema fixes that are still needed: reset stale `execution_state` on load (section 7); and,
  ideally, JS `moveCells` keeping `execution_state` (or a real Y move), so the `[*]` does not blink off
  when a queued cell is moved. Correctness does not depend on this, since the proxy re-mirrors after
  observing a move.

### Clean (notebook schema 2.0.0 → 2.1.0, or 3.0.0)
- `execution_state: 'idle' | 'queued' | 'running'` (widen `IExecutionState`, `api.ts:537`). This is a
  TS type change that is backward-compatible for readers who only check `=== 'running'`. A 2.0 frontend
  would show a queued cell as not running, so this deserves the minor bump plus a version check in the
  frontend.
- Optionally a proxy-owned per-cell `execution: Map{exec_id, status: 'ok'|'error'|'aborted'|'cancelled'|'lost',
  source_sha}` for the "last run" badge and "edited since run". Needs `_cell_to_py` to pop it
  (`ynotebook.py:120`), `_update_cell` to leave it alone on reload (`:631`), and JS `toJSON`/`createCell`
  / `moveCells` to carry it, or a root map keyed by cell id (variant D storage, F3) instead, which avoids
  all three.
- Real Y move in `moveCells` (Yjs move support, `ynotebook.ts:253` FIXME), which removes the E5 repair
  path.

What *must* change in jupyter_ydoc in either variant: the new `YKernelSession` doc (Python + JS), and a
reset of stale `execution_state` on load. What *can* stay as-is: the notebook's nbformat round-trip,
`meta`, `cells` structure, and the save path.

## 9. Open questions for the user

- **Q1 Abort scope.** On error or interrupt, abort only the failing *batch* (proposed for errors), or
  the whole kernel queue as today (proposed for interrupt)? It matters with several users or notebooks
  on one kernel.
- **Q2 Window = 1 vs pipelining.** Accept one shell round trip per cell for cancellable, proxy-ordered
  queues? Or pipeline and let the kernel abort?
- **Q3 Staleness policy for offline intents.** Reject only on epoch/session mismatch (proposed), or also
  apply a TTL (e.g. 60 s by client `ts`, which is clock-skew sensitive)? Run silently, or ask the user?
- **Q4 Same cell queued twice.** Run twice (today's parity, proposed), or coalesce into one?
- **Q5 When to clear outputs.** At start (proposed: cancelled cells keep old outputs), or at queue time
  (today)?
- **Q6 Lost executions after a proxy crash.** Mark `lost` with a synthetic stderr/output note in the cell?
  Leave outputs as-is? Or just reset to idle?
- **Q7 Minimal vs clean.** Is a notebook schema minor bump (`'queued'` state) acceptable, or must the
  notebook doc stay byte-for-byte schema-compatible, with fine state only in the session doc?
- **Q8 Session doc granularity.** One per kernel (proposed; console + notebooks share), or one per
  notebook session (simpler auth and linking, but no shared queue)?
