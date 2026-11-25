# 01: How execution works today, and prior art

Scope: the current Y schema for notebooks, how jupyter-collaboration hosts rooms, the JupyterLab execution path, and every prior attempt at "server-side execution". The last of these are jupyverse, jupyter-collaboration's executor, jupyter-server-nbmodel, jupyter-server-documents and the 2026 JEP pre-proposal. All paths are relative to `/home/user/refs/`.

Snapshot versions:
- jupyter_ydoc 4.2.1 (py + `@jupyter/ydoc` 4.2.1, commit 5886dec)
- jupyter-collaboration at commit 441f030
- JupyterLab 4.7.0-beta.2 (commit 585894c)
- jupyter-server-nbmodel 0.2.10
- jupyverse 0.14.21
- pycrdt-websocket 0.16.9
- jupyter-server-documents at commit 3078760, cloned for this note
- server-side-documents at commit 07842eb, cloned for this note

---

## 1. The Y schema (jupyter_ydoc)

### 1.1 Root types (one `Y.Doc` per notebook room)

| root key | Y type | contents | where |
|---|---|---|---|
| `state` | `Y.Map` | `dirty: bool`, `path: str`, `hash: str`, `document_id: str` (room name, set by the frontend), and anything else | `jupyter_ydoc/ybasedoc.py:42`, `javascript/src/ydocument.ts:21` |
| `meta` | `Y.Map` | `nbformat: number`, `nbformat_minor: number`, `metadata: Y.Map` (notebook metadata; the values inside are **plain JSON**) | `ynotebook.py:69,365-368`, `ynotebook.ts:99` |
| `cells` | `Y.Array<Y.Map>` | one `Y.Map` per cell | `ynotebook.py:70` |

Python `YNotebook.version` is `"2.0.0"` (`ynotebook.py:73-81`). The schema docstring is at `ynotebook.py:32-56`.

### 1.2 Cell map (`Y.Map`), as built by `create_ycell` (`ynotebook.py:175-202`) and JS `createCell` (`ycell.ts:59-110`)

| key | type | notes |
|---|---|---|
| `id` | str | uuid4 if missing |
| `cell_type` | str | `code` / `markdown` / `raw` |
| `source` | **`Y.Text`** | |
| `metadata` | **`Y.Map`** | top-level keys only; nested values are plain JSON |
| `attachments` | plain JSON | markdown/raw cells only |
| `execution_count` | number \| null (plain) | Yjs gives a float; Python casts it back (`ynotebook.py:121`). The JS getter is `get('execution_count') \|\| null` (`ycell.ts:764`), so 0 becomes null. |
| `execution_state` | str (plain) | `'running' \| 'idle'` in JS (`api.ts:537`). Python sets `"idle"` on creation (`ynotebook.py:200`). It is **stripped on serialisation** (`ynotebook.py:120`), so it lives only in the Y doc and the YStore, never in the .ipynb file. jupyverse writes `"busy"`, which is outside the JS enum (`jupyverse/plugins/kernels/src/fps_kernels/kernel_driver/driver.py:132`). |
| `outputs` | **`Y.Array<Y.Map>`** | code cells only. Each output is a `Y.Map` whose values are **plain JSON** (`data`, `metadata`, `traceback`, …), with one exception: **stream `text` is a `Y.Text`** (`ynotebook.py:204-219`, `ycell.ts:818-841`). |

Additional observations:
- The JS `createOutputs` joins a list-form stream text with `text.join()`, which uses a comma separator (`ycell.ts:827`). This looks like a minor bug; Python uses `"".join` (`ynotebook.py:216`).
- Writers do not consistently produce `Y.Map` outputs. nbmodel appends plain dicts for non-stream outputs (`jupyter-server-nbmodel/jupyter_server_nbmodel/actions.py:284`), and jupyter-server-documents appends plain dicts for everything, streams included (`jupyter-server-documents/jupyter_server_documents/outputs/output_processor.py:96-107`). JupyterLab tolerates both forms: its comment says "For compatibility with older ydoc where a plain object… In a future major release the use of Map will be required" (`jupyterlab/packages/cells/src/model.ts:892-895`).
- jupyverse appends a whole **`Y.Doc` (subdocument)** into `outputs` for ywidgets (`driver.py:259-268`). This is the only prior art that nests a subdoc in the notebook doc. pycrdt has only just stabilised subdocument identity (pycrdt commit cff905c, "Use the same object for a subdocument every time it is accessed (#490)").

### 1.3 Mutation helpers and change events (JS `YCodeCell`)

- `setOutputs`, `updateOutputs(start, end, outputs, origin)`, `clearOutputs(origin)`, `appendStreamOutput(index, text, origin)` and `removeStreamOutput(index, start, origin)` are at `ycell.ts:846-928`.
- Every mutator sets `notebook.dirty = true` (`ycell.ts:780,852,869,885,914,928`). **Dirty is set by client-side API calls, not by observing the doc.** A server-side writer that uses raw pycrdt does *not* set `state.dirty` unless it does so explicitly.
- `CellChange` exposes `outputsChange` (an Array delta), `streamOutputChange` (the Text delta on path `['outputs', i, 'text']`), `executionCountChange` and `executionStateChange` (`ycell.ts:946-990`).
- Origins: `'silent-change'` marks changes that came from the local OutputAreaModel, so they are not echoed back (`ycell.ts:690`).

### 1.4 Version history relevant to us (`jupyter_ydoc/CHANGELOG.md`)

| version | change |
|---|---|
| 1.1.0a0 | awareness interface (#171) |
| 2.0.0 | switch from y-py to pycrdt (#194) |
| 3.0.0 | **`execution_state` on cells (#197)**, **stream outputs as `Y.Text` (#201)**, `appendStreamOutput`/`removeStreamOutput` (#241), `streamOutputChange` (#264), execution_state exposed in JS (#259), awareness inside the document (#277, #282) |
| 3.1.0 | `clearOutputs()` (#330) |
| 3.2.0 | `document_id` preserved in `state` across `set()` (#338) |
| 3.3.1 | a cell is hard-reloaded if its outputs change and include a stream (#362) |
| 4.0.0 | **breaking: dirty state set on document change (#406)**, pycrdt 0.13 then 0.14 |
| 4.1.0 | progressive loading, with large outputs delayed (#418) |
| 4.2.0 | granular source updates (#450) |
| 4.2.1 | state-change events emitted after content observers finish (#454) |

### 1.5 Gotchas for a kernel peer that writes to this schema

- **`YNotebook._set()` deletes every `state` key except `dirty`, `path` and `document_id`** (`ynotebook.py:392-393`). `_set()` runs on every out-of-band reload from disk (`jupyter-collaboration/projects/jupyter-server-ydoc/jupyter_server_ydoc/rooms.py:401-402`). Kernel state kept in the notebook's `state` map would therefore be wiped by an external change. This argues for a separate doc or a separate root map.
- `_update_cell` returns `False` (meaning "hard replace the cell map") when new outputs contain a stream (`ynotebook.py:589-597`). A disk reload during execution therefore replaces the `Y.Map` of any cell with stream output. **A peer holding a reference to the old `ycell` keeps writing into a detached map.** Look cells up by id each time, or observe `cells` for replacement.
- When a room is rebuilt from the YStore, `execution_state` is restored too. A stale `"running"` can survive a server crash. When the room is rebuilt from disk, every cell is `"idle"` (`ynotebook.py:200`).
- Rooms built from disk use `Doc(client_id=0)` so their history is deterministic (`rooms.py:251-282`). A kernel peer must use its own random client id.

---

## 2. jupyter-collaboration (jupyter-server-ydoc)

### 2.1 Rooms

- The room id is `"{format}:{type}:{file_id}"` (`utils.py:69-87`), for example `json:notebook:<uuid>`. Clients obtain the file id with `PUT /api/collaboration/session/<path>` (`handlers.py:513-566`), which also returns `SERVER_SESSION`, a uuid generated per server process (`utils.py:22`).
- The websocket is `/api/collaboration/room/<room_id>` (`app.py:195-211`).
- `DocumentRoom(YRoom)` (`rooms.py:28`) wraps a `jupyter_ydoc` document around `room.ydoc` and `room.awareness` (`rooms.py:54`). It observes changes to drive autosave (`rooms.py:81,441-477`).
- `TransientRoom` is used for awareness-only rooms such as `JupyterLab:globalAwareness` (`rooms.py:553`, `handlers.py:153-163`).

### 2.2 Loading and saving

- `initialize()` (`rooms.py:117-225`) loads the file, then tries the YStore (SQLite `.jupyter_ystore.db` by default, `stores.py:21-44`). If the YStore content differs from the file, it reloads from the file. Otherwise it uses the store.
- Saving is debounced by `document_save_delay` (1 s by default, `app.py:82-88`). `_maybe_save_document` (`rooms.py:491-550`) calls `aget()`, writes through the FileLoader, then updates `dirty` and `hash`.
- Autosave is skipped if every awareness state has `autosave: false` (`rooms.py:456-471`). **A kernel peer with no awareness state is treated as `autosave: True`.**
- Out-of-band changes: polling every `file_poll_interval` (1 s). In `overwrite` mode the room calls `aset()` with the disk content (`rooms.py:378-433`).

### 2.3 Room lifetime: the main constraint for an in-process kernel peer

- `on_close()` schedules `_clean_room()` when **`room.clients == {self}`**, i.e. when the last websocket client leaves (`handlers.py:403-413`).
- After `document_cleanup_delay` (60 s by default, `app.py:74-80`) the room is deleted (`handlers.py:435-464`). `DocumentRoom.stop()` **cancels the pending save task** (`rooms.py:304-305`).
- `room.clients` holds only websocket channels. In-process writers (nbmodel, and any in-process kernel peer) are invisible to it.
- Consequence today: if the browser closes during a long run, the room is torn down 60 s later. Outputs written after that go into an orphaned `YNotebook` and are lost, and edits made in the final second before teardown may never be saved. nbmodel has this problem: it holds the document returned by `get_document(copy=False)` (`actions.py:73-85`).
- **A kernel peer must pin the room**: register as a client, or add a refcount, or set `document_cleanup_delay=None`. This needs a small change upstream.

### 2.4 The server is already a Y peer

- `YDocExtension.get_document(room_id=… | path=…, copy=False, create=True)` returns the **live** `YNotebook` of the room, creating the room if needed (`app.py:231-329`). With `copy=True` it returns a fork made from `get_update()`. This is the hook nbmodel uses.
- Server-side edits made through pycrdt are broadcast to all clients by `YRoom._broadcast_updates` (`pycrdt-websocket/src/pycrdt/websocket/yroom.py:197-220`).
- Server awareness: the room's `awareness` local state is broadcast to clients (`yroom.py:399-425`). A kernel peer could publish presence or status this way, for example `{"user": {"name": "Python 3 kernel"}}`.
- Out-of-process option: `pycrdt.Provider(doc, channel)` (`pycrdt/python/pycrdt/_provider.py:77`) syncs a local `Doc` over any `Channel`. `YRoom(provider_factory=…)` (`yroom.py:40-46,293-300`) can mirror a room to an external peer. A proxy process could therefore join the room through the same websocket the browsers use.

### 2.5 Frontend provider

- The `YWebsocketProvider` connects with `sessionId` (`packages/docprovider/src/yprovider.ts:295-313`).
- **After the first sync, the frontend writes `state.document_id = roomname`** (`yprovider.ts:543-547`). Every server-side executor uses this value to find the room (`notebookCellExecutor.ts:97`). It was added in jupyter-collaboration PR #307, which renamed the key from `room_id` to `document_id`.

### 2.6 Server-side execution switch

- The `YDocExtension.server_side_execution` trait (`app.py:115-121`) is passed to the page config as `serverSideExecution` (`app.py:161`).
- The frontend plugin `@jupyter/docprovider-extension:notebook-cell-executor` (`packages/docprovider-extension/src/executor.ts:16-31`) provides `INotebookCellExecutor`. It returns `NotebookCellServerExecutor` when the flag is on, and otherwise the stock `runCell`.
- `NotebookCellServerExecutor.runCell` (`packages/docprovider/src/notebookCellExecutor.ts:35-137`) runs the same SessionContext checks as the default executor (terminating, `pendingInput`, `hasNoKernel`, and starting a kernel if needed). It then sends **`POST api/kernels/{id}/execute` with `{cell_id, document_id}` and no code**. On a 2xx it calls `onCellExecuted({success: response.ok})`, so "executed" means "accepted". There is a `FIXME` about deletedCells and timing (`:106`).
- This body matches **jupyverse's** API, `Execution{document_id, cell_id}` (`jupyverse/api/kernels/src/jupyverse_kernels/models.py:38-40`). It does **not** match nbmodel, which needs `code` in the body. nbmodel therefore ships its own executor plugin.
- The kernel websocket stays connected in this mode, which keeps the busy/idle indicator working. This was a stated design choice in jupyterlab PR #15448 (see §5).

---

## 3. The JupyterLab execution path today (4.7.0-beta.2)

### 3.1 Call chain

1. **Command → `NotebookActions.run*`**. For example, `runCells` (`packages/notebook/src/actions.tsx:776-796`) calls `Private.runCells` (`actions.tsx:3357-3439`).
   - `Private.runCells` uses **`Promise.all(cells.map(runCell))`**: every selected cell's request starts at once. No frontend queue serialises them.
   - It shows a "kernel initializing" notification (`:3369-3396`).
   - On a `KernelReplyNotOK` error it resets `[*]` to idle on cells that have no execution count (`:3418-3428`).
2. **`Private.runCell`** (`actions.tsx:3476-3503`) builds `IRunCellOptions` and calls **`executor.runCell(options)`**.
   - `executor` is a module global set once by `setCellExecutor` (`actions.tsx:2723-2728`), which throws if called twice.
   - `setCellExecutor` is called from `activateNotebookHandler` with the `INotebookCellExecutor` dependency (`packages/notebook-extension/src/index.ts:2252-2269`).
   - The default provider is `@jupyterlab/notebook-extension:cell-executor` (`notebook-extension/src/cellexecutor.ts:10-15`).
   - **Swapping execution behaviour = disable that plugin and provide `INotebookCellExecutor`.** This is the hook all prior art uses: jupyter-collaboration, nbmodel and jupyter-server-documents (`jupyter-server-documents/src/index.ts:61-68`). `INotebookCellExecutor` was introduced as `ICellExecutor` in jupyterlab PR #15830 (merged March 2024, JL 4.2). The interface is `runCell(IRunCellOptions): Promise<boolean>`, where the options are `cell` (the widget), `notebook` (the model), `notebookConfig`, `onCellExecuted`, `onCellExecutionScheduled`, `sessionContext`, `sessionDialogs` and `translator` (`notebook/src/tokens.ts:144-204`).
   - **Limitation:** the hook only covers notebook "run cell". It does not cover consoles, complete, inspect, interrupt, restart, kernel status, stdin, comms/widgets or the debugger. Each of those still goes through `SessionContext` → `KernelConnection` over the websocket.
3. **Default `runCell`** (`notebook/src/cellexecutor.ts:25-146`):
   - Runs the SessionContext checks: `isTerminating`, `pendingInput` (refuses to run while an `input()` is pending, `:56-65`), and `hasNoKernel` (starts or selects a kernel).
   - Calls `onCellExecutionScheduled`.
   - Calls `CodeCell.execute`.
   - On reply `ok` it handles the `set_next_input` payload, which inserts or replaces a cell from the client side (`:156-203`). Otherwise it throws `KernelError`.
4. **`CodeCell.execute`** (`packages/cells/src/widget.ts:1799-1913`):
   - With empty code or no kernel it calls `clearExecution()`.
   - Otherwise, in a `'silent-change'` transaction, `clearExecution()` clears outputs, sets count to null and state to idle (`model.ts:696-704`).
   - Then **`model.executionState = 'running'`** (`widget.ts:1828`). Note the comment "in future we would like to distinguish running from scheduled" (`:1827`): **there is no `queued` state.** A queued cell shows `[*]` exactly like a running one.
   - Then `model.trusted = true` and `OutputArea.execute(...)`.
   - `recordTiming` adds an iopub message hook that writes `metadata.execution`.
   - After the reply: **`model.executionCount = reply.content.execution_count`** (`:1878`).
   - On error with the same future it sets `executionState = 'idle'` (`:1899-1901`).
5. **Who sets `idle` on success?** `CodeCell.onStateChanged`: **when `executionCount` becomes non-null, the *cell widget* writes `executionState = 'idle'`** (`widget.ts:1663-1669`). This runs in every client that renders the cell, so every client writes the kernel-owned field.
   - jupyter-server-documents hit this. It defers writing `execution_count` until completion because "Writing it immediately triggers executionCountChange on the frontend which sets executionState='idle'" (`jupyter-server-documents/jupyter_server_documents/rooms/ynotebook_room.py:320-338`).
   - Other frontend writers of `executionState`: `actions.tsx:2825,2842,3086,3118,3428`, `widget.ts:1901`, `model.ts:709`. **"Only the proxy writes kernel-owned state" is therefore not true today without frontend changes.**
6. **`OutputArea.execute`** (`packages/outputarea/src/widget.ts:1049-1075`):
   - Builds `{code, stop_on_error}`. `stop_on_error` is false if the cell has the `raises-exception` tag.
   - Calls `kernel.requestExecute(content, false, metadata)`, where metadata carries the `cellId` and the cell metadata.
   - Sets `output.future = future`. The `future` setter (`widget.ts:192-247`) clears the model and attaches `onIOPub`, `onReply` and `onStdin`.
7. **`KernelConnection.requestExecute`** (`packages/services/src/kernel/default.ts:792-824`):
   - Defaults: `silent:false`, `store_history:true`, `allow_stdin:true`, `stop_on_error:false`. `session` is the connection's `clientId`.
   - Calls `sendShellMessage`, then `_sendMessage` (`:467-505`), which **buffers messages while disconnected or restarting**.
   - The websocket is `api/kernels/<id>/channels?session_id=<clientId>` (`default.ts:1434-1456`). The kernel queues requests on its shell channel.

### 3.2 How iopub reaches the shared model

- `OutputArea._onIOPub` (`outputarea/src/widget.ts:829-876`):
  - `execute_result`, `display_data`, `stream` and `error` → `model.add(output)`.
  - `clear_output` → `model.clear(wait)`.
  - `update_display_data` → `model.set(i, …)` for each index in a **per-OutputArea `_displayIdMap`**.
  - `status: idle` → `_pendingInput = false`.
- The execute reply's `page` payload becomes a display_data output (`:880-908`).
- **Cross-cell display updates** are routed at the *kernel connection* level. `_handleDisplayId` maps `display_id` to its parent msg_ids and re-dispatches the message as `update_display_data` to those futures (`services/src/kernel/default.ts:1190-1240`). This is frontend-memory state. A server peer must rebuild it as `display_id → [(cell_id, output_index)]`.
  - nbmodel does not support `update_display_data` (`actions.py:291-293`, `# FIXME`).
  - jupyter-server-documents supports it only through its outputs service index (`output_processor.py:84-104`).
- `OutputAreaModel.add/_add` (`outputarea/src/model.ts:276-374`) merges consecutive streams with the same name and applies **terminal semantics for `\r` and `\b`** (`Private.processText` / `addText`, `model.ts:545-660`). Progress bars depend on this.
- **Sync from OutputAreaModel to Y** (`cells/src/model.ts:768-855`):
  - `add`/`set`/`remove` → `updateOutputs(..., 'silent-change')`.
  - `clear` → `clearOutputs()`.
  - Stream text edits → `removeStreamOutput` / `appendStreamOutput`.
  - **The Y.Text therefore holds the post-`\r` processed text, not the raw kernel bytes.**
- **Sync from Y to OutputAreaModel** (`cells/src/model.ts:859-926`):
  - `streamOutputChange` → `appendStreamOutput` / `removeStreamOutput` on the **last** output, whatever index changed (`outputarea/src/model.ts:253-266`).
  - `outputsChange` inserts → `this._outputs.add(...)`, which **always appends** ("Inserting an output always results in appending it", `cells/src/model.ts:889-896`). It ignores the insert position, and `add()` merges consecutive streams again.
  - A remote writer must therefore only append at the end, or replace-and-clear. An insert in the middle or a write to a non-last stream renders wrongly on remote clients.
  - nbmodel reimplements `processText` on the server (`_apply_terminal_controls`, `actions.py:138-183`) and writes minimal Y.Text diffs with common-prefix logic (`actions.py:255-272`).
- Remote `execution_count` and `execution_state` changes emit `stateChanged` (`cells/src/model.ts:901-917`), which updates the prompt.

### 3.3 Stdin

- `onStdin` → `OutputArea.onInputRequest` (`outputarea/src/widget.ts:574-…`) creates a **widget-only Stdin panel**. Nothing is written to the model.
- The reply goes through `future.sendInputReply` (`widget.ts:1344`, `default.ts:1017-1028`).
- `SessionContext.pendingInput` (`apputils/src/sessioncontext.tsx:462`) blocks new runs.
- The prompt exists only in the browser that sent the request. It is not collaborative and does not survive a reload.

### 3.4 SessionContext and kernel status

- `SessionContext` (`apputils/src/sessioncontext.tsx:370`) owns the `/api/sessions` session, through `sessionManager.connectTo` (`:846-858`) and `startKernel`/`changeKernel`/`restartKernel` (`:717-792`).
- Status signals: `statusChanged` and `kernelDisplayStatus` (`:455,613`) are derived from iopub `status` messages on **the websocket** and the connection status.
- `iopubMessage` and `unhandledMessage` (`:476-484`) feed consoles, the log console and debugger plumbing.
- The kernel picker, the status bar and the execution indicator all read SessionContext. For "no UI change", either keep the websocket for status (what nbmodel and jupyter-collaboration do) or provide a Y-backed `Kernel.IKernelConnection`/`IKernelManager`.
- In JL ≥4.4 the service managers are pluggable: `@jupyterlab/services-extension` provides `IKernelManager`, `ISessionManager` and others as replaceable plugins (`packages/services-extension/src/index.ts:199-287`). This second hook could remove the websocket entirely without touching the UI.

### 3.5 Other frontend-only execution state that a Y design makes unnecessary

- JL 4.7 moves *running futures* across cell move, delete and undo (`captureExecution`, `actions.tsx:2942-2991`). It buffers iopub, stdin and reply messages while a cell is detached, and restores the `displayIdMap` (`:3042-3118`).
- If outputs are keyed by cell id on the server, this machinery becomes unnecessary.

---

## 4. jupyter-server-nbmodel (Datalayer), 0.2.10

### 4.1 API

Implemented in `jupyter_server_nbmodel/extension.py:54-93` and `handlers.py`.

- `POST /api/kernels/{kid}/execute` with body `{code, metadata:{cell_id, document_id, document_path, record_timing}, server?:{url, token}}`. Returns **202** with `Location: /api/kernels/{kid}/requests/{uid}` (`handlers.py:47-108`). The `server` field lets it drive a kernel on a *remote* Jupyter server through `jupyter_kernel_client` (`execution_stack.py:138-161`).
- `GET /api/kernels/{kid}/execute` lists pending requests (`handlers.py:35-45`).
- `GET …/requests/{uid}` returns 200 (done), 202 (pending), **300 (input pending; `Location` is `/input`)** or 500 (`handlers.py:152-190`).
- `DELETE …/requests/{uid}` cancels. A queued request is dequeued. A running request gets an interrupt, then is abandoned after `cancel_grace_period` (`handlers.py:192-225`, `execution_stack.py:314-371`).
- `POST /api/kernels/{kid}/input` with `{input}` (`handlers.py:111-140`, `execution_stack.py:400-428`).
- `GET/PUT /api/nbmodel/settings/output-recovery` (`handlers.py:293-335`).

### 4.2 Execution model

- There is **one server-side `asyncio.Queue` and one worker per kernel**, kept in memory (`execution_stack.py:92-94,177-209`). Execution is strictly serialised: one `execute_interactive` at a time (`actions.py:640-731`).
- The queue is **not in the document** and is lost on server restart.
- Stop-on-error drains the queue (`actions.py:690-697`).
- `dedup_task_queue` keeps only the latest pending request per cell, to handle "restart & run all" (`actions.py:584-637`).
- The frontend serialises submissions per kernel (`src/submissionQueue.ts`), and the body carries **code captured at click time** (`src/executor.ts:115,145-157`). This avoids the stale-source problem but trusts the client's snapshot.
- The kernel client gets its **own session id**. Otherwise `ROUTER_HANDOVER` steals the shell identity from other clients (`execution_stack.py:108-128`). This matters for any proxy that shares a kernel with browser websockets.
- If the kernel goes idle without a reply, the client is reconnected after `execute_reply_timeout` (`actions.py:383-434`).

### 4.3 How it writes to the Y doc

- `_get_ycell` resolves the live doc through `ydoc_extension.get_document(room_id=document_id, copy=False)` and/or by path, then finds the cell by id with a linear scan (`actions.py:39-111`).
- At start, in one transaction, it clears outputs and sets `execution_count=None` and `execution_state="running"`. It writes the **request handle into `metadata.jupyter_server_nbmodel`** so a reloaded browser can resume polling (`actions.py:468-488`, `src/executor.ts:354-390`). That handle leaks into the .ipynb if the file is saved mid-run.
- Streams: terminal-control emulation plus a minimal Y.Text diff (§3.2).
- Other outputs: appended as plain dicts, with deduplication against the HTTP reconciliation path (`actions.py:273-284`). The frontend has a fallback that **also** writes outputs fetched over HTTP into the doc (`src/outputReconciliation.ts`), so there are two writers.
- `clear_output(wait=True)` is treated as immediate (`actions.py:285-290`, FIXME). `update_display_data` is ignored.
- At the end, in one transaction, it sets `execution_count` and `execution_state="idle"` and records timing.

### 4.4 Stdin

- `_stdin_hook` stores **one `PendingInput` per kernel** (`actions.py:296-320`).
- The poller sees 300 and shows a Stdin widget **only in the browser that submitted** (`src/requestServer.ts:105-190`, with the TODO at `:130`).
- The reply is sent over REST. Nothing is in the doc, so it is not collaborative and is fragile on reload. The reload problem is noted in jupyter-collaboration PR #307.

### 4.5 What it does not solve

- The kernel websocket stays open for status, comms/widgets, complete, inspect, interrupt, restart and the debugger.
- No queue or "queued" state in the document. No multi-client ordering guarantee beyond per-browser serialisation.
- Room lifetime (§2.3). No `update_display_data`. Partial widget support: the README says "We are working to make it work with ipywidgets".
- My inference about widgets: comm_open/comm_msg still reach the browser through its own kernel websocket, so models are created in the browser. The `application/vnd.jupyter.widget-view+json` output is in the doc, but the model state is not persisted, so widgets are lost on reload.
- No authority model: outputs are written by both the server and the browser fallback.

---

## 5. Other prior art

### 5.1 jupyverse (David Brochart): the first server-side execution, REST-triggered with a server-side kernel driver

- Route: `POST /api/kernels/{id}/execute` with `{document_id, cell_id}` (`api/kernels/src/jupyverse_kernels/__init__.py:93-99`, `models.py:38-40`).
- Handler (`plugins/kernels/src/fps_kernels/routes.py:384-413`): gets the room (`yjs.get_room(document_id)`), clears outputs, lazily creates a `KernelDriver` connected to the kernel's connection file, then calls `driver.execute(ycell, wait_for_executed=False)`.
- `KernelDriver` (`kernel_driver/driver.py`) is a pure-anyio ZMQ client:
  - `execute()` sets `execution_state="busy"`, sends `execute_request` with the **source read from the Y doc** (`str(ycell["source"])`), and **does not wait**. Several requests can be in flight; the kernel queues them (`driver.py:123-178`).
  - iopub is routed by `parent_header.msg_id` (`:103-112`).
  - `_handle_outputs` appends stream text to the last `Y.Text` if the name matches, with **no `\r` processing**, and appends other outputs as dicts (`:241-286`).
  - On idle followed by the reply, it sets `execution_count` and `execution_state="idle"` in one transaction (`:180-192`).
  - No stdin, no `clear_output`, no `update_display_data`.
- **ywidgets**: `comm_open` with `target_name=="ywidget"` creates a ypywidgets model doc in the server and syncs it with the kernel over comm buffers that carry Yjs sync messages (`plugins/yjs/src/fps_yjs/widgets.py:24-62`, `driver.py:194-206,289-311`).
  - A display with `application/vnd.jupyter.ywidget-view+json` causes the widget doc to be served as its own room `ywidget:{guid}` and **appended into `outputs` as a subdoc** (`driver.py:259-268`).
  - Brochart, in jupyter_server#1274: "there will be a YDoc representing the widget in the kernel, in the server and in the client".
  - He demoed full notebook state recovery including widgets with jupyverse and jpterm (Nov 2023, jupyterlab#2833 and jupyter_server#1274).
- Enabling it: `jupyverse --set kernels.require_yjs=true --set jupyterlab.server_side_execution=true` (`jupyter-collaboration/docs/source/configuration.md:72-89`).
- Related QuantStack pieces:
  - **ypywidgets**: a CRDT-backed partial replacement for ipywidgets that uses comm as transport and pycrdt for state.
  - **yjs-widgets**: the JupyterLab renderer for ypywidgets.
  - **akernel**: an anyio kernel that can run in-process in jupyverse (`task` and `thread` modes), so the kernel can live in the server's process.
  - **jpterm**: a terminal client.
- Solved: outputs survive browser close. The kernel side is a doc peer, and widget state is in CRDTs (for ywidgets only).
- Limitations: jupyverse only; standard ipywidgets are not covered; no stdin; no queue state; streams have no terminal semantics; `"busy"` is outside the JS enum.

### 5.2 jupyterlab PR #15448 → `INotebookCellExecutor` (#15830) → jupyter-collaboration PR #279 (merged 2024-04-11) and PR #307 (merged 2024-07-12)

- #15448 ("Support server-side execution", Nov 2023) was closed in favour of putting the executor in jupyter-collaboration.
- The pluggable executor (jtpio's suggestion) became #15830.
- Naming debate: "server-side-execution", "server-side-composition" and "server-side-outputs".
- The kernel websocket was intentionally kept for the busy indicator, as "an incremental step toward removing the frontend's kernel WebSocket protocol".
- Known limitation at the time: widgets were not recovered.
- #307 added `state.document_id`. Its discussion produced the **stdin debate**:
  - krassowski: a special-cased stdin box is justified but must survive refresh.
  - Brochart and fcollonval: worried that putting kernel requests into the CRDT doc leaks kernel state into the document model.
  - Brochart proposed a **dedicated stdin output type**.
  - krassowski called stale source sent with the request a blocker. "Restart & run all" executed out of order.
- jupyterlab#2833 ("Reconnect to running session: keeping output", opened 2017) was closed by #279.

### 5.3 jupyter_server#900 "New kernels REST API" (Brochart, 2022, open) and #1274 "Restore notebook execution progress on reload" (2023, open)

- #900:
  - Thesis: with RTC the UI is "merely a shared document editor", so the backend should run code and write outputs into the doc, with kernel state sent over the event system.
  - Zsailer: add `POST …/execute`, but do not remove the websocket API, because other projects depend on it.
  - ipywidgets were acknowledged as not working.
  - cben (2025) noted that nbmodel's API differs from the proposal because it sends code inline.
- #1274: proposals included decoupling the kernel from the websocket session_id and a server-side notebook model.
  - fcollonval raised `input()`.
  - sa- noted that "a YDoc in the kernel would also work with gateways".
  - krassowski (June 2025): it "mostly works" through nbmodel or jupyverse. Open gaps: tqdm multi-line output, ipywidgets, timing of scheduled execution.
  - In November 2026 the thread was pointed to the tracking issue #1736.

### 5.4 jupyter-server-documents (jupyter-ai-contrib; Zsailer and others): a room-owned kernel client

- `YNotebookRoom(YRoom)` **owns the kernel client** (`rooms/ynotebook_room.py:88-145`). It has a room-level `asyncio.Queue` and worker, and handles kernel restart and death callbacks (`:187-197`).
- `POST /api/kernels/{id}/execute` takes `{document_id, cells:[{cell_id, source_hash}], client_id?, request_id?, previous_request_id?}` (`execution_handlers.py:16-97`).
  - **All cells are hash-verified, then enqueued atomically**. A mismatch returns 409 and enqueues nothing (`ynotebook_room.py:369-445`).
  - It sets `execution_state="running"` at enqueue time, so queued and running are again indistinguishable.
  - At run time the **source is re-read from the doc** (`:349-353`). The hash is checked only at enqueue.
  - **`allow_stdin=False`** (`:352`): no input() at all.
- `OutputProcessor` (`outputs/output_processor.py`) writes **plain-dict outputs, one per stream message**. Merging is left to the frontend's `add()`. It handles `clear_output(wait)` with a pending set, and `update_display_data` through the outputs-service index.
- The optional **outputs service** stores outputs on disk and puts **URL placeholders** in the Y doc (`outputs/manager.py:18-69,314-…`). This is one answer to output volume: the doc stays small and outputs are lazy-loaded.
- Ships a frontend `INotebookCellExecutor` that computes murmur2 (`src/index.ts:54-120`, `src/murmur2.ts`).

### 5.5 2026 standardisation effort (current and directly relevant)

- **jupyter_server#1736**, "Tracking issue: Server-side documents and execution plan" (opened 2026-11-18). Goals: CRDT-backed server-side document state and server-side execution as a core experimental API in both jupyter-server and jupyverse. It commits to a JEP, weekly meetings, and a Playwright suite from Apple.
- Planning repo: `jupyter-server/server-side-documents`, cloned to `server-side-documents/`.
  - `execute-api.yaml`: `POST /api/kernels/{kernel_id}/execute`, body `{document_id, cells:[{cell_id, source_hash}], client_id?, sequence?, request_id?}`. Responses: 200 with body `null`, or 409.
  - `document.md`: "The REST endpoint only requests execution. Its results are delivered by writing to the cell in the shared (CRDT) document."
- **JEP pre-proposal, jupyter/enhancement-proposals#157** (Brochart, 2026-11-17):
  - Standardises that REST API.
  - `source_hash` is MurmurHash2 with seed 0, as a decimal string.
  - Per-(doc, kernel, client) `sequence`: 400 below the expected value; `sequence:0` resets, and waiters get 409 `reset`; a 10 s gap timeout gives 409 `timeout`; a kernel disconnect resets the sequence, an in-place restart preserves it.
  - "Does not replace the messaging protocol". Client-driven execution stays supported.
  - **It is silent on kernel status, stdin, widgets, schema changes and kernel-as-peer.**
  - The follow-up is JEP PR #158 (2026-11-24).
- **Implication for this design:** the community is converging on *REST to request, CRDT to deliver*. The "intent in the doc" design differs only in how the request travels. If execution requests are expressed as Y edits, the proxy can reuse the same server-side queue and semantics (source hash, sequence) and remain compatible with the JEP. The two front doors could coexist.

### 5.6 Other related work (not examined in depth)

- `jupyter_kernel_executor` (Wh1isper): execution over HTTP; on hold (per jupyter_server#1274).
- Ganimede: a third-party notebook whose kernel manager syncs a Python YDoc with the frontend.
- VS Code's "IPyWidget Architecture Challenges" wiki explains why widget state lives in the frontend.
- No JEP or ipykernel issue proposes the **kernel itself** (or a proxy colocated with it) as a direct Y peer. jupyter-collaboration#247 argues that "the backend is just another CRDT peer".

---

## 6. Summary of what each piece solves

| | Trigger | Who queues | Queue visible in doc | Streams | `\r` handling | display updates | stdin | widgets | kernel status |
|---|---|---|---|---|---|---|---|---|---|
| JupyterLab default | WS execute_request per cell, all at once | kernel | no (`running` = queued) | frontend → Y.Text | frontend | conn-level map | browser-local widget | browser | WS iopub |
| jupyverse | REST `{doc, cell}` | kernel (fire-and-forget) | no | Y.Text append | no | no | no | ywidgets subdoc | WS |
| jc executor | same REST as jupyverse | n/a | no | n/a | n/a | n/a | refuses if pending | n/a | WS |
| nbmodel | REST `{code, meta}` + poll | server queue per kernel | no (metadata handle) | Y.Text diff | server-side emulation | no | REST 300 + one browser | partial (browser) | WS |
| server-documents | REST `{doc, cells+hash}` | room queue | no (`running` at enqueue) | dict per msg | frontend on render | via outputs index | disabled | ? | WS |
| JEP #157 | REST `{doc, cells+hash, seq}` | server | unspecified | unspecified | unspecified | unspecified | unspecified | unspecified | unspecified |

Sources (web): jupyterlab#2833, jupyterlab#15448, jupyter-collaboration#279 and #307, jupyter_server#900, #1274 and #1736, jupyter/enhancement-proposals#157, github.com/QuantStack/ypywidgets, github.com/davidbrochart/akernel, github.com/jupyter-server/server-side-documents, and the jupyter-ai-contrib/jupyter-server-documents repo.
