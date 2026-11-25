# 04 — Frontend integration: running cells through the Y doc with no UI change

Scope: how JupyterLab's frontend would send execution through the notebook Y doc and a side
"kernel session" Y doc instead of the kernel websocket, while the user still picks a kernel exactly as
today. Refs: JupyterLab `main` @ 585894c (4.7.0-beta.2, sparse checkout), jupyter-collaboration
(frontend packages), jupyter-server-nbmodel (Datalayer fork, frontend in `src/`), jupyter_ydoc JS
(`javascript/src`), ipywidgets `python/jupyterlab_widgets/src` (cloned shallow for this note).
All paths below are relative to `/home/user/refs/`.

The side-doc vocabulary (`status`, `queue`, `control`, `requests`/`replies`, `stdin`/`stdin_replies`)
matches `/home/user/design/prototype/kyproxy/schema.py`.

---

## 0. TL;DR

* **Every strategy needs `INotebookCellExecutor` replaced.** The default path
  (`runCell` → `CodeCell.execute` → `OutputArea.execute`) is the main frontend writer of
  kernel-owned state, and it is static code. The executor token is the only supported seam
  that keeps it from running without forking JupyterLab. nbmodel and jupyter-collaboration
  already use it.
* A kernel-connection shim (strategy b/c) on its own **cannot** remove the double writes:
  `CodeCell.execute` writes `execution_count`, `execution_state`, `trusted` and `metadata.execution`
  itself, and `OutputArea` writes whatever IOPub it is fed into the shared model.
* Recommended plan, in two phases:
  1. **Phase 1 (strategy a+):** add a Y-intent `INotebookCellExecutor`, a stdin renderer, a status
     bridge and small guards. Keep the real `KernelConnection` websocket for
     completion, inspection, history, comms, the debugger and the console. No JupyterLab fork.
     About 900 frontend LOC.
  2. **Phase 2 (strategy c):** add a custom `IKernelManager` (a `ServiceManagerPlugin`, JupyterLab
     4.4 and later) whose `connectTo` returns a `YKernelConnection`. It serves status,
     complete/inspect/history/is_complete/kernel_info, interrupt/restart and the execution signals
     from the side doc. It opens a real websocket lazily, only for consumers that still need one:
     the debugger, the console, and comms until the widgets design lands. About 1,000 to 1,300 LOC
     more.
* The frontend's Y→model reader has three bugs that become visible once *every* client is a
  "remote" reader (§3.3). Either the proxy works within those limits or JupyterLab gets a small
  core patch, about 40 LOC in `CodeCellModel._onSharedModelChanged`.

---

## 1. Inventory: frontend code paths that talk to the kernel today

Legend for "Change": **E** = the replacement executor removes it; **K** = keep it on the real websocket
in Phase 1; **Y** = served from the side doc in Phase 2 (YKernelConnection); **R** = REST only, already
independent of the websocket; **W** = writes kernel-owned state into the Y doc (see §3).

| # | Code path | Where (file:line) | What it does with the kernel | Change |
|---|---|---|---|---|
| 1 | Executor dispatch | `jupyterlab/packages/notebook/src/actions.tsx:3488-3515` (`Private.runCell` → `executor.runCell` or `defaultRunCell`); `runCells` with `Promise.all` at `:3357-3440` | Every run command (run, run-all, run-and-advance, restart-and-run…) goes through here | Unchanged. This is the plug point |
| 2 | Executor plugin + token | token `notebook/src/tokens.ts:145-206`; default plugin `notebook-extension/src/cellexecutor.ts:10-18`; consumed by the tracker plugin `notebook-extension/src/index.ts:428-436` and `setCellExecutor(executor)` `:2269` (`actions.tsx:2724`, which throws if set twice) | Supplies `runCell` | **E**: provide our own plugin and disable `@jupyterlab/notebook-extension:cell-executor` (and `@jupyter/docprovider-extension:notebook-cell-executor`), as nbmodel does in `jupyter-server-nbmodel/package.json:121-124` |
| 3 | Default `runCell` | `notebook/src/cellexecutor.ts:25-146` | Guards (`isTerminating`, `pendingInput`, `hasNoKernel` → `startKernel`/`selectKernel`), then `CodeCell.execute`, then `handlePayload` (`set_next_input`, `:156-203`) | **E W** (`handlePayload` edits source or inserts a cell) |
| 4 | `CodeCell.execute` | `cells/src/widget.ts:1800-1925` | `clearExecution` (`:1819-1827`), `executionState='running'` (`:1829`), `trusted=true` (`:1830`), `OutputArea.execute` (`:1838`), timing hook on the future (`:1845-1872`), `executionCount = reply.execution_count` (`:1879`), timing (`:1880-1895`), abort handling (`:1897-1912`). Passes `cellId` and `deletedCells` in the request metadata (`:1813-1817`, `cellexecutor.ts:80-93`) | **E W** |
| 5 | `OutputArea.execute` + future handlers | `outputarea/src/widget.ts:1050-1077` (`kernel.requestExecute`); `_setFuture` clears the model (`:230`) and wires `onIOPub`/`onReply`/`onStdin` (`:242-252`); `_onIOPub` adds/clears/sets outputs (`:830-880`); `_onExecuteReply` turns pager payloads into outputs (`:882-922`); stdin widget + echo (`:585-640`) | Writes every output into `OutputAreaModel`; `CodeCellModel.onOutputsChange` mirrors them into Y (`cells/src/model.ts:798-857`) | **E W** |
| 6 | Undo/redo of cells with an execution in flight (new in 4.7) | `notebook/src/actions.tsx:458, 657, 1760, 3751, 3863` (`captureExecution`), `:2788-3200` (detach/reattach futures, replay buffered IOPub, rewrite `executionCount`, `executionState`, `metadata.execution`) | Keeps futures alive across delete, move and undo | Becomes a no-op: `captureExecution` returns null without `outputArea.future`. The proxy's cell-id addressing replaces it |
| 7 | `SessionContext` | `apputils/src/sessioncontext.tsx:370-1290`; proxies `session.statusChanged`, `connectionStatusChanged`, `iopubMessage`, `unhandledMessage`, `pendingInput` (`:1037-1046`, `:1166-1241`); `kernelDisplayStatus` (`:613-660`); `restartKernel` (`:756-777`); created per document in `docregistry/src/context.ts:82-91` (no token, so it cannot be swapped) | Status, restart, kernel selection | Kernel selection: **R** (sessions REST, unchanged). Status: **K** in Phase 1, since the kernel broadcasts IOPub `status` to every websocket subscriber, including busy states caused by proxy executions. **Y** in Phase 2 |
| 8 | Session to kernel connection creation | `services/src/session/default.ts:317` (`_connectToKernel`) → `session/manager.ts:343-347` → `kernel/manager.ts:127-160` (`new KernelConnection`, whose constructor opens the socket immediately at `kernel/default.ts:67`) | Opens `/api/kernels/<id>/channels` | Phase 2 override point (`IKernelManager`, §2c) |
| 9 | Kernel status widgets | toolbar `apputils/src/toolbar/widget.tsx:183-195`; status bar `apputils/src/kernelstatuses.tsx:215-230`; notebook plugin `notebook-extension/src/index.ts:1098` | Listen to `sessionContext.statusChanged`/`connectionStatusChanged` | Nothing changes if `SessionContext` gets a correct status (§4) |
| 10 | Busy favicon | `SessionContext._onStatusChanged` → `setBusy` (`sessioncontext.tsx:1181-1192`) → `application/src/status.ts:75` → `application-extension/src/index.tsx:1826-1840` | Follows the kernel status | Free once the status is correct |
| 11 | Execution indicator | `notebook/src/executionindicator.tsx:380-460`: counts outgoing `execute_request` seen on `kernel.anyMessage` (`:410-438`); completes them on IOPub `status: idle` whose parent `msg_id` matches (`:420-427`, `:532-541`); `NotebookActions.executionScheduled` (`:56`) | Progress, elapsed time | Phase 1: **breaks partially**, because the browser never sends `execute_request` and so no ids match. Fix with an adapter plugin (§5) or accept. Phase 2: YKernelConnection emits synthetic `anyMessage` (send `execute_request` with `msg_id=req_id`, then IOPub `status idle` with that parent), so it works unchanged |
| 12 | Kernel restart | commands `notebook-extension/src/index.ts:5894`, `console-extension/src/index.ts:1475`; dialogs `sessioncontext.tsx:1511, 1549`; `KernelConnection.restart` = REST `/restart` + `reconnect()` (`services/src/kernel/default.ts:548-566`) | REST, then a websocket reconnect | **R**. The proxy must notice the restart and bump `status.generation`, cancelling queued intents. Phase 2 also offers the `control.restart` intent |
| 13 | Kernel interrupt | `notebook-extension/src/index.ts:3670-3690`, `console-extension/src/index.ts:1177`, `apputils/src/toolbar/widget.tsx:52`; `KernelConnection.interrupt` = REST (`kernel/default.ts:521-527`) | REST | **R**. It already works without the websocket. The proxy must handle the abort replies for queued cells |
| 14 | Completer (kernel provider) | `completer/src/default/kernelprovider.ts:28-54` (`requestComplete`), `:113` (`requestInspect` for docs); registered in `completer-extension/src/index.ts:62` | shell `complete_request` | **K** → **Y** (`requests`/`replies`). Alternative: register a Y-backed `ICompletionProvider` and drop `KernelCompleterProvider` |
| 15 | Inline history completer | `completer/src/default/inlinehistoryprovider.ts:88, 130` | `history_request` | **K** → **Y** |
| 16 | Tooltip | `tooltip-extension/src/index.ts:340-360` (`requestInspect`) | `inspect_request` | **K** → **Y** |
| 17 | Inspector panel | `inspector/src/kernelconnector.ts:35-47` | `inspect_request` | **K** → **Y** |
| 18 | Kernel history (notebook) | `notebook/src/history.ts:77, 341` | `history_request` | **K** → **Y** |
| 19 | "Run in console"/`is_complete` selection | `notebook-extension/src/index.ts:2085-2100` | `is_complete_request` | **K** → **Y** |
| 20 | Language info / kernelspec metadata | `notebook/src/panel.ts:54-61, 224-290` (`kernel.info` → `setMetadata('language_info')`, `kernel.spec` → `setMetadata('kernelspec')`); LSP adapter `notebook/src/notebooklspadapter.ts:428` | `kernel_info_request` | **K** → **Y** (the proxy stores the `kernel_info` reply in `status`). **W**, mild (§3) |
| 21 | Log console | `notebook-extension/src/nboutput.ts:73-82` (`sessionContext.iopubMessage` / `unhandledMessage`) | Mirrors IOPub into the log | Phase 1: still receives the broadcast IOPub. Phase 2: only the synthetic messages (status, execute_input, error). Acceptable |
| 22 | Console execution | `console/src/widget.ts:870-905` → `IConsoleCellExecutor` (`console/src/tokens.ts:69`) → `console/src/cellexecutor.ts:55` (`CodeCell.execute`); `is_complete` at `console/src/widget.ts:989`; history at `console/src/history.ts:305`; foreign handler on IOPub at `console/src/foreign.ts:23` | Full kernel protocol, **no Y doc** (console cells are local models) | **K** in both phases (justified: no shared document to write into, nothing to persist). A future "console as a Y doc" could reuse the executor approach through `IConsoleCellExecutor` |
| 23 | Debugger | `debugger/src/session.ts:78-82, 301` (`requestDebug` on control, `iopubMessage` for `debug_event`); `debugger/src/handler.ts:175-213` (status, iopub, `anyMessage`); `debugger/src/service.ts:188-205` (kernelspec `debugger` flag) | DAP over control channel plus IOPub events | **K** (justified: an interactive, per-user, stateful protocol with nothing to persist). Breakpoints still hit for proxy-run cells because it is the same kernel, as long as the proxy sends the same `cellId` metadata so `dumpCell` file names line up |
| 24 | ipywidgets manager | `ipywidgets/python/jupyterlab_widgets/src/manager.ts:87` (`registerCommTarget`), `:126` (`createComm`), `:141` (`requestCommInfo`), `:346-349`, `:427-431` (status); Output widget `output.ts:60-78` (`registerMessageHook(msg_id)`); state save `manager.ts:446-466` | Comms over the websocket | Phase 1: **K**. `comm_open`/`comm_msg` from proxy-run code are broadcast on IOPub, so browser widget views still appear. The Output widget's message hook keys on the execute `msg_id`, which the browser never sees, so the proxy (server-side widget model) must capture outputs instead. Saving state is **W**. Phase 2: comm bridge through the side doc; see the widgets note |
| 25 | Kernel selector / session | `sessioncontext.tsx:717-745, 935-1010` (sessions REST) | REST | **R**, unchanged. This is how "no UI change" is met |

Not kernel-related but relevant: `NotebookActions.clearOutputs` and `clearAllOutputs`
(`actions.tsx:1896-1940`) call `clearExecution()`, a user-intent write of kernel-owned fields (§3).

---

## 2. Integration strategies compared

### (a) Replace only `INotebookCellExecutor` (the nbmodel / jupyter-collaboration approach)

What exists today:
* jupyter-collaboration: `docprovider-extension/src/executor.ts:16-31` returns a REST executor when
  `PageConfig.serverSideExecution` is on (`projects/jupyter-server-ydoc/jupyter_server_ydoc/app.py:115, 161`).
  `docprovider/src/notebookCellExecutor.ts:36-137` POSTs `{cell_id, document_id}` and lets the server
  write into the room.
* nbmodel (Datalayer fork), `jupyter-server-nbmodel/src/`:
  * `plugin.ts:194-234`: executor plugin.
  * `executor.ts:300-600`: POST with `cell_id`, `document_id`, `document_path` and `record_timing`;
    polls; reconciles outputs from the REST reply when Y updates did not arrive (`:534-570`).
  * `submissionQueue.ts`: keeps submission order per kernel, because `runCells` fires every
    `runCell` concurrently through `Promise.all`.
  * `requestServer.ts:105-211`: stdin over REST, rendering a `Stdin` widget into the output area.
  * `kernelStatus.ts:677-716`: forces the kernel status through the **private**
    `KernelConnection._updateStatus` after a reload.
  * `plugin.ts:97-192`: resumes executions after a reload.

  It still writes outputs, `execution_count` and `execution_state` from the client in its reconcile
  paths (`executor.ts:425, 567-569, 611, 627-629`), so it is a double writer by design.

For us the executor turns the click into a Y intent in the side doc `queue` and waits for the proxy
to mark it terminal. The kernel websocket stays open for everything else.

* Pros:
  * Supported public token with no fork. Small.
  * Removes every **execution-path** writer: §1 rows 3, 4, 5 and 6 vanish.
  * Status, busy favicon, completer, tooltip, inspector, widgets and debugger keep working
    unchanged, because the kernel broadcasts IOPub, including `status`, to every subscriber of the
    browser's existing websocket.
* Cons:
  * The websocket stays open, so the brief's "no websocket" ideal is not met. Only execution
    moves.
  * The browser receives each output twice, once as IOPub on the websocket and once as Y
    updates. Mitigation: a server-side filtering kernel websocket connection that drops output
    IOPub (`stream`, `display_data`, `execute_result`, `error`, `clear_output`,
    `update_display_data`) for clients in Y mode. jupyter_server ≥2 lets the websocket connection
    class be configured (`ServerApp.kernel_websocket_connection_class`, from memory; not verified
    here because jupyter_server is not in the venv or refs).
  * The execution indicator breaks partially (row 11).
  * After a reload the status shows `unknown` until the next IOPub status. Today nbmodel needs the
    private-API hack for this.
  * `pendingInput` cannot be set on `SessionContext` (it comes from `kernel.hasPendingInput`).
    The executor checks the side doc `stdin` map instead.

### (b) A Y-backed `Kernel.IKernelConnection` shim, used as the real connection

Implement `Kernel.IKernelConnection` (`services/src/kernel/kernel.ts:44-620`, about 50 members:
`status`, `connectionStatus`, `info`, `spec`, `request*`, `sendShellMessage`/`sendControlMessage`,
comms, message hooks, subshells, `clone`, `reconnect`, `restart`, `interrupt`, `shutdown`, plus the
signals `statusChanged`, `connectionStatusChanged`, `iopubMessage`, `anyMessage`, `unhandledMessage`,
`pendingInput`, `disposed`). Requests become Y intents. Y updates are turned back into fake
`KernelMessage`s.

How it maps:
* `requestExecute(content, _, metadata)` → `queue[req_id] = {cell_id: metadata.cellId, …}`.
  It returns an `IShellFuture` whose `msg.header.msg_id = req_id`. The future emits synthetic
  `status busy`, `execute_input` and `status idle` IOPub, and an `execute_reply` built from the
  proxy's terminal record.
* `requestComplete`, `requestInspect`, `requestHistory`, `requestIsComplete`, `requestKernelInfo` →
  `requests[rid]`, resolved from `replies[rid]`.
* `interrupt`/`restart` → REST as today, or `control` intents.
* `status` → `status.execution_state`. `connectionStatus` → the Y provider's state combined with
  whether the proxy is present (§4).

What breaks if the shim is used *with the default executor*:
* **Double writes of outputs.** If the future forwards output IOPub, `OutputArea._onIOPub` writes
  each output into `OutputAreaModel`, which `CodeCellModel.onOutputsChange` mirrors into Y
  (`cells/src/model.ts:798-857`). The proxy writes the same outputs, so they appear twice. If the
  future withholds output IOPub (outputs then arrive only through Y), `OutputArea._setFuture`
  still calls `model.clear()` (`outputarea/src/widget.ts:230`). That clear is harmless under
  Yjs: it deletes only the item ids it knows, so concurrent proxy inserts survive. But…
* **`CodeCell.execute` still writes** `clearExecution`, `executionState='running'`, `trusted`,
  `executionCount` and the timing metadata (§3.1). These are Y.Map last-writer-wins sets that
  race the proxy. Example: a client's `executionState='running'`, applied concurrently with the
  proxy's final `'idle'`, can win the conflict depending on client-id ordering, and leave `[*]`
  stuck forever.
* `display_id` handling (`kernel/default.ts:1614-1631`, `OutputArea._displayIdMap`) is per widget.
  The proxy has to own it anyway.
* stdin: `OutputArea.onInputRequest` needs an `IStdinMessage` on `future.onStdin` and calls
  `future.sendInputReply` (`outputarea/src/widget.ts:248-252`, `:1344`). The shim can synthesize
  both from the side doc `stdin` map. The echo (`:624`) is then another frontend write.
* Consumers with no Y doc (console, `requestExecute` calls from other extensions, the
  `ipywidgets` Output widget hook keyed by `msg_id`) need real IOPub outputs. The shim must either
  stream outputs through the side doc for executions with no `cellId` (a new ephemeral channel) or
  delegate them to a real websocket.

Verdict: feasible, and the right way to make status, completer, tooltip, inspector, history and the
execution indicator work *unchanged*. It does not solve execution on its own; the executor
replacement is still required.

### (c) Custom `IKernelManager` / `ISessionManager` / `IServiceManager` plugin so the connection is virtual

This is how (b) gets installed without a fork. Since JupyterLab 4.4 the service managers are
plugins: `services/src/tokens.ts:32` (`ServiceManagerPlugin<T> = IPlugin<null, T>`), tokens
`IKernelManager` (`:107`), `ISessionManager` (`:139`), `IServiceManager` (`:155`), `IServerSettings`
(`:131`). The default plugins are in `services-extension/src/index.ts:199-219` (kernel manager),
`:262-285` (session manager) and `:384-420` (service manager). `dev_mode/index.js:330-366` registers
federated extensions' plugins *before* resolving `IServiceManager`, so a prebuilt extension can
provide `IKernelManager`. Disable `@jupyterlab/services-extension:kernel-manager` with
`disabledExtensions`, as nbmodel does for the executor.

* `YKernelManager extends KernelManager`: override `connectTo` (`kernel/manager.ts:127-160`) to
  return a `YKernelConnection` when the kernel is proxied (a page-config flag plus the side doc
  `status` showing the proxy is present), and otherwise `super.connectTo`. `startNew`, `running`,
  `shutdown` and `refreshRunning` stay REST.
* Gap: `connectTo` only gets `{id, name}`, not the document. The side doc should therefore be
  **per kernel** (room `kernel:<kernel_id>`), and execute intents carry `cell_id` plus the notebook
  room id (`notebook.sharedModel.getState('document_id')`, as nbmodel does at `executor.ts:366`).
  If the path is needed, also override `ISessionManager` (`session/manager.ts:343` builds
  `connectToKernel`) to pass the session path through.
* `YKernelConnection` keeps a **lazy delegate** real `KernelConnection`. It is opened on the first
  call that cannot be served from Y: `requestDebug`, `requestExecute` without a notebook cell
  (console, extensions), comms until the widget design is done, subshells. This bounds the
  semantics: Y where there is a document or side-doc equivalent, and the real wire otherwise. The
  debugger and console keep working, and the brief's "keep a websocket only where justified"
  becomes an actual code path.
* With (c), the notebook still uses the executor from (a), because `CodeCell.execute` must not run.
  The executor can be thin: it calls `yKernel.enqueue(cell)` and listens to the
  `YKernelConnection`'s future. The execution indicator, status and log console then work through
  the synthetic messages.

Risks:
* `IKernelConnection` is large and partly implicit: `clone()`, `dispose`-on-`dead`
  (`kernel/default.ts:1272-1283`), `info` must resolve before `SessionContext` reports ready, and
  `KernelConnection` has instance checks or private access in places (nbmodel's `_updateStatus`
  hack).
* Every extension that does `session.kernel.requestExecute` and expects outputs would silently go
  through the delegate. That is correct, but it means the websocket is rarely zero in practice.
* `ServiceManagerPlugin`s need JupyterLab ≥4.4. Notebook 7 shares the same plugin system.

### (d) Other options

* **(d1) Fake `WebSocket` constructor in `IServerSettings`** (`services/src/serverconnection.ts:103-110`).
  Emulate the kernel wire protocol for `/api/kernels/*/channels` URLs, backed by Y. Everything
  above runs unmodified, but it has the same double-writer problem as (b) plus serialization
  overhead. Note that the docprovider passes `serverSettings.WebSocket` to y-websocket
  (`jupyter-collaboration/packages/docprovider/src/yprovider.ts:300-309`), so the fake class must
  pass other URLs through. Prototyping only.
* **(d2) Server-side interception, zero frontend change.** A custom kernel websocket connection
  class on the server recognises `execute_request` messages carrying `metadata.cellId` from a
  collaborative session. It converts them into a Y intent, answers with the proxy's
  `execute_reply`, and suppresses output IOPub to that client. Unmodified frontends (stock
  JupyterLab, Notebook 7) get durable outputs. But `CodeCell.execute` still writes
  `executionState`, `executionCount`, `trusted` and timing (identical values, but LWW races,
  §3.1). Useful as a **compatibility mode** for clients without the extension, not as the
  target.
* **(d3) Swap `SessionContext`.** Not possible without a fork or replacing the document
  context: it is constructed inside `docregistry/src/context.ts:82`.
* **(d4) Custom `NotebookPanel.IContentFactory` / cell model factory.** A cell model subclass could
  fix the Y→model reader bugs in §3.3 without patching core. It is heavier than an upstream
  patch, but possible today.

### Comparison

| | Fork-free token(s) | Removes execution double writes | Status / favicon | Completer, tooltip, inspector | Execution indicator | Websocket still open | Size |
|---|---|---|---|---|---|---|---|
| (a) executor only | `INotebookCellExecutor` | yes | yes, via broadcast IOPub; reload needs a bridge | yes (ws) | partially broken | yes | ~900 LOC |
| (b) shim alone | none (no install point) | **no** | yes | yes | yes | no | — |
| (c) = (a) + (b) installed through `IKernelManager` | `INotebookCellExecutor`, `IKernelManager` (+`ISessionManager`) | yes | yes, natively | yes (Y) | yes | lazily (debugger, console, comms) | +1,000–1,300 LOC |
| (d1) fake WebSocket | `IServerSettings` | no | yes | yes | yes | no | large |
| (d2) server interception | server config | partially | yes | yes | yes | yes | server only |

---

## 3. The double-writer problem: every frontend write of kernel-owned state

### 3.1 Inventory

| Field | Writer | file:line | Disable by |
|---|---|---|---|
| `outputs` (clear before run) | `CodeCell.execute` → `clearExecution` | `cells/src/widget.ts:1819-1827`; `cells/src/model.ts:706-714` | Executor never calls `CodeCell.execute`. The proxy clears at execution start |
| `outputs` (clear) | `OutputArea._setFuture(clear=true)` | `outputarea/src/widget.ts:230` | No future is ever set (executor) |
| `outputs` (add/merge/set/clear_output) | `OutputArea._onIOPub` → `OutputAreaModel` → `CodeCellModel.onOutputsChange` / `_connectStreamListener` | `outputarea/src/widget.ts:830-880`; `cells/src/model.ts:762-785, 798-857` | Same |
| `outputs` (pager payload as display_data) | `OutputArea._onExecuteReply` | `outputarea/src/widget.ts:882-922` | Same. The proxy should add the `page` payload as `display_data` to keep parity |
| `outputs` (stdin echo `name: 'stdin'`) | `OutputArea.onInputRequest` | `outputarea/src/widget.ts:615-628` (also nbmodel `requestServer.ts:173`) | Our stdin renderer must **not** add the echo; the proxy writes it |
| `source` / new cell (`set_next_input`) | `handlePayload` | `notebook/src/cellexecutor.ts:156-203` | The proxy applies it (it persists with the browser closed); it is in the `execute_reply` payload |
| `execution_count` | `CodeCell.execute` | `cells/src/widget.ts:1879`, also `clearExecution` | Executor |
| `execution_state` `running` | `CodeCell.execute` | `cells/src/widget.ts:1829` | Executor. `[*]` latency becomes one round trip to the proxy; optionally add a purely local CSS "pending" class |
| `execution_state` `idle` (reaction) | `CodeCell.onStateChanged`: a non-null `executionCount` sets state `idle` **on every client that receives it** | `cells/src/widget.ts:1666-1669` | Cannot be disabled without a patch. Neutralise it: the proxy sets `execution_count` and `execution_state='idle'` **in the same transaction**, so the getter already returns `idle` when observers run and the equality guard in `ycell.ts:790-796` suppresses the write |
| `execution_state` `idle` (error path) | `runCells` catch on `KernelReplyNotOK` | `notebook/src/actions.tsx:3420-3430` | The executor never rejects with a `KernelError`. It calls `onCellExecuted({success:false, error})` and returns `false` |
| `execution_state` / `execution_count` / `metadata.execution` (undo of moved/deleted running cells) | `syncExecutionStateWithFuture`, `restoreExecution`, `restoreTiming` | `notebook/src/actions.tsx:2812-2930, 3030-3200` | A no-op without futures (`captureExecution` returns `null`) |
| `execution_state` `idle` (console) | console error path | `console/src/widget.ts:893-897` | Console stays on the websocket and does not use the notebook Y doc. Not applicable |
| `metadata.execution` (timing) | `CodeCell.execute` hooks | `cells/src/widget.ts:1845-1909`; `clearExecution` deletes it (`model.ts:711`) | Executor. The intent carries `record_timing` (from `notebookConfig.recordTiming`, as nbmodel does at `executor.ts:383, 402`). The proxy writes `iopub.status.busy`, `iopub.execute_input`, `shell.execute_reply.started`, `shell.execute_reply` and `execution_failed` from the kernel message header dates |
| `metadata.trusted` | `model.trusted = true` → `CodeCellModel.onTrustedChanged` writes `metadata.trusted` | `cells/src/widget.ts:1830`; `cells/src/model.ts:733-752`; `clearExecution` `:713` | Keep it, but as a **user intent**: the executor sets `cell.model.trusted = true` for the requesting user before enqueueing. The proxy never touches trust. Caveat: other clients only read `metadata.trusted` at model construction (`model.ts:245`) and never react to remote changes, so collaborators see a cell someone else ran as untrusted. That is the same as today's RTC. Fix with about 15 LOC: listen to `metadataChanged` for `trusted` |
| `metadata.language_info`, `metadata.kernelspec` | `NotebookPanel._updateLanguage/_updateSpec` on every kernel change, from every client | `notebook/src/panel.ts:224-290` | Low risk: idempotent, same value. Long term the proxy owns `language_info` (it holds the `kernel_info` reply). No change in Phase 1 |
| `metadata.widgets` (widget state) | jupyterlab_widgets `_saveState` on save, from every client | `ipywidgets/python/jupyterlab_widgets/src/manager.ts:446-466` | Setting `saveState: false` (checked at `:447`). Server-side widget state is persisted by the proxy (widgets note) |
| `outputs`, `execution_count`, `execution_state` (user clear) | `clearOutputs` / `clearAllOutputs` → `clearExecution` | `notebook/src/actions.tsx:1896-1940` | A **user intent**, so allowed. Deleting outputs is safe under Yjs, since only known items are deleted and concurrent proxy appends survive. The only race is `executionState='idle'` written while the cell is still running: cosmetic, and corrected by the proxy's next write. Proper fix: make `clearExecution` leave `executionState` alone when called by a user command (upstream), or route "clear" through a side-doc intent |

nbmodel itself, being a REST design, keeps a reconcile path that writes outputs, `execution_count`
and `execution_state` (`executor.ts:425, 567-569, 611-630`). Its own comments explain why
(`executor.ts:417-423`: replacing outputs through the shared model "can expose a half-built nested
value"). Our executor must not copy those paths: in the Y design the single source is the room.

### 3.2 Enforcement (defence in depth)

The server is the real gatekeeper (it can reject or revert non-proxy updates touching `outputs`,
`execution_count` or `execution_state`; see the server-side notes). On the frontend, add a
development-mode guard. It is an `observeDeep` on each code cell `ymodel` that logs, or in strict
mode reverts, **local-origin** transactions touching those keys, with an allowlist for user clears.
About 60 LOC. It quickly finds stray writers in third-party extensions.

### 3.3 The Y→model reader becomes the only path, and it has holes

Today the executing client renders from its own `OutputArea`, so remote-reader bugs only affect
collaborators. In the new world **every** client reads outputs through
`CodeCellModel._onSharedModelChanged` (`cells/src/model.ts:860-904`) and
`YCodeCell.getChanges` (`jupyter_ydoc/javascript/src/ycell.ts:948-975`).

1. **Mid-array inserts are appended at the end.** The `insert` branch always calls
   `this._outputs.add(...)` (`model.ts:893-900`), ignoring `retain`. Any non-tail replacement,
   such as `update_display_data` targeting an earlier output, is reordered on screen.
2. **Stream changes are applied to the last output only, and only one per transaction.**
   `getChanges` takes the *first* `outputs/<i>/text` event (`ycell.ts:951-961`), and the model
   applies it with `appendStreamOutput`/`removeStreamOutput` on the **last** output
   (`outputarea/src/model.ts:250-268`), ignoring `<i>` and the delta's `retain`.
3. **Local post-processing diverges from Y.** `OutputAreaModel._add` merges consecutive streams of
   the same name and applies `\r`/`\b` (`outputarea/src/model.ts:340-395`) on the reader side,
   inside `globalModelDBMutex`, so nothing is written back. If the proxy writes two adjacent
   stdout entries or raw `\r` text, the local list or text length no longer matches Y, and later
   index-based deltas hit the wrong place.

Constraints this puts on the proxy (until patched upstream): merge consecutive same-name stream
outputs into one `Y.Text`; apply terminal controls server-side
(`prototype/kyproxy/schema.py: process_text`); express rewrites as tail-delete plus append; touch at
most one stream text per transaction; update displays in place by mutating the output `Y.Map`'s
`data`/`metadata` keys instead of delete+insert. That last one is still not observed (only
`outputs` array and `text` events are), so (1) really needs a patch.

**Upstream patch (recommended, about 40 LOC in `@jupyterlab/cells`):** honour `retain` for inserts
(add `OutputAreaModel.insert(index, output)`) and for stream deltas (index by path), handle all
events in a transaction, and react to in-place output-map changes. That gives correct
`update_display_data` for every reader, including today's RTC collaborators.

---

## 4. Kernel status display

**Source of truth:** side doc `status.execution_state` (`starting|idle|busy|restarting|dead`), plus
`generation`, `kernel_name`, `language` and the cached `kernel_info` reply. All are written only by
the proxy. Proxy liveness comes from the proxy's **awareness** entry in the side room: no entry
means "no proxy".

**Mapping to `Kernel.Status` / `ConnectionStatus`:**
* `connectionStatus` = Y provider status (`connecting`/`connected`/`disconnected`) AND proxy present
  (synced and no proxy means `connecting`).
* `status` = `status.execution_state`, or `unknown` when not synced.
* `kernelDisplayStatus` (`sessioncontext.tsx:613-660`) is derived by `SessionContext` from these and
  needs no change.

**Feeding `SessionContext`:**
* **Phase 1 (real websocket kept):**
  * The status arrives as it does today, through IOPub broadcast, including busy and idle caused
    by proxy executions. Kernel status widgets, the busy favicon (`sessioncontext.tsx:1181-1192`)
    and `panel.ts:244-268` auto-restart dialogs all work.
  * The only gap is the first seconds after a (re)load, before any IOPub status. Bridge it from
    the side doc. The only public-ish lever is nbmodel's call to
    `KernelConnection._updateStatus` (private, `services/src/kernel/default.ts:1272`; nbmodel
    `kernelStatus.ts:677-692`).
  * Alternative without private API: show the side-doc status in our own status widget, and leave
    the core one at `unknown` for a moment.
  * Also use the side doc for "is the proxy alive" UX: when the proxy is absent and cells are
    queued, show a notification.
* **Phase 2 (`YKernelConnection`):**
  * `statusChanged` and `connectionStatusChanged` are emitted from Y observers. `SessionConnection`
    forwards them (`session/default.ts:337-390`) to `SessionContext._onStatusChanged`
    (`sessioncontext.tsx:1166-1195`), and every widget, the favicon and the restart dialogs follow
    without changes.
  * Restart: the `restarting` → `starting` → `idle` sequence and the `generation` bump map to what
    `SessionContext.restartKernel` expects (`:756-777`).
  * `autorestarting` (kernel died) must be published by the proxy as well. `panel.ts:250-262` shows
    the dialog.
* **Per-notebook fallback:** if a per-kernel room is not wanted, the proxy can mirror the status into
  the notebook doc's existing `ystate` map (`jupyter_ydoc/javascript/src/ydocument.ts:21, 150-167`).
  That map already carries `dirty`, `path` and `document_id`. One kernel serving several documents
  then means several writes, which is acceptable.

Opening the side room from the frontend: jupyter-collaboration already joins a non-file room,
`JupyterLab:globalAwareness`, through `IAwarenessProviderFactory`
(`collaboration-extension/src/collaboration.ts:87-110`; `docprovider/src/awareness.ts:25-37`). A
`kernel:<id>` room needs a server-side room type next to the special cases in
`projects/jupyter-server-ydoc/jupyter_server_ydoc/handlers.py:115-161`. The frontend uses
y-websocket directly, or a small factory, with
`URLExt.join(wsUrl, 'api/collaboration/room')` (`docprovider/src/yprovider.ts:38, 281-309`).

---

## 5. Minimal frontend change set

One prebuilt extension (`@jupyter/kernel-yproxy`, a working name), gated by a page-config flag
written by the server extension, as `serverSideExecution` is today. Package-level
`disabledExtensions`: `@jupyterlab/notebook-extension:cell-executor`,
`@jupyter/docprovider-extension:notebook-cell-executor` (and in Phase 2
`@jupyterlab/services-extension:kernel-manager`).

### Phase 1 (meets "execution through the Y doc"; no fork)

| File / plugin | Responsibility | ~LOC |
|---|---|---|
| `kerneldoc.ts`: `KernelDocRegistry` | Opens and caches the side room per kernel id; typed accessors for `status`, `queue`, `stdin`, `replies`; proxy presence from awareness; `whenTerminal(req_id)` | 200 |
| `executor.ts`: plugin providing `INotebookCellExecutor` | Same guards as the default `runCell` (`cellexecutor.ts:44-78`). Allocates `seq` **synchronously, before any `await`**, so `runAll` order holds (nbmodel needed `submissionQueue.ts` for this). Sets `trusted` for the requesting user. Writes `queue[req_id] = {cell_id, doc: document_id, seq, client, gen, record_timing, stop_on_error (from the `raises-exception` tag, as in `outputarea/src/widget.ts:1056-1064`), deleted_cells, batch}`. Emits `onCellExecutionScheduled`; awaits the terminal state; calls `onCellExecuted` with a `KernelError`-shaped error; returns `false` on error, cancel or skip and never throws. If the proxy is absent and the flag is off, falls back to the stock `runCell` | 250 |
| `stdin.ts`: plugin (requires `INotebookTracker`) | For each `stdin[req_id]` whose `cell_id` is in an open notebook, mounts `Stdin` (from `@jupyterlab/outputarea`) into the cell's output area with a fake future whose `sendInputReply` writes `stdin_replies[req_id]` (pattern from nbmodel `requestServer.ts:129-211`). Removes it when the record is deleted. **Does not** add the echo output. Any collaborator can answer | 150 |
| `status.ts`: plugin | Pushes the side-doc status into the kernel connection on (re)connect (bridge, private API, guarded), plus a "proxy offline, N cells queued" notification | 80 |
| `guards.ts` | Trust sync on remote `metadata.trusted` (§3.1); dev-mode local-write guard (§3.2) | 80 |
| `indicator.ts` (optional) | Execution indicator fed from the `queue`: disable `@jupyterlab/notebook-extension:execution-indicator` (`notebook-extension/src/index.ts:577`) and provide an equivalent that counts `queue` records for this document | 120 |
| `index.ts`, settings schema, page-config gate | Wiring | 60 |
| **Total** | | **~940** |

Server-side items this phase depends on (other notes): the `kernel:<id>` room type; the proxy
writing `execution_count` and `idle` atomically, merging streams, processing `\r`, applying
`set_next_input` and pager payloads, and writing timing; and optionally the filtering kernel
websocket.

### Phase 2 (no websocket unless justified)

| File / plugin | Responsibility | ~LOC |
|---|---|---|
| `ykernel.ts`: `YKernelConnection implements Kernel.IKernelConnection` | Status, info and spec from the side doc. `requestComplete/Inspect/History/IsComplete/KernelInfo` through `requests`/`replies`. `requestExecute` with a notebook `cellId` → `queue` future with synthetic non-output IOPub and reply. `interrupt`/`restart` through REST or `control`. Synthetic `anyMessage` for the execution indicator. **Lazy delegate** `KernelConnection` for `requestDebug`, cell-less execute (console, extensions), comms and subshells | 800–1,000 |
| `ykernelmanager.ts`: `ServiceManagerPlugin` providing `IKernelManager` | `class extends KernelManager`, overriding `connectTo` | 80 |
| `ysessionmanager.ts` (only if the path is needed) | Passes the session path to `connectTo` | 60 |
| Executor slims down to `yKernel.enqueue` | | −100 |
| Comm bridge (with the widgets design) | `createComm`/`registerCommTarget` over side-doc maps, with the proxy as the comm peer | 300+ |

### Optional upstream patches to JupyterLab (small and justified on their own)

1. `CodeCellModel._onSharedModelChanged`: honour `retain` for output inserts and stream deltas,
   handle all events in a transaction, observe in-place output-map changes (§3.3). About 40 LOC,
   in `@jupyterlab/cells` and `@jupyterlab/outputarea`.
2. `CodeCellModel`: react to remote `metadata.trusted` (`model.ts:245, 383`). About 10 LOC.
3. `clearExecution`: do not reset `executionState` from user clear commands (`actions.tsx:1908, 1937`).
   About 5 LOC.
4. A public `Kernel.IKernelConnection` status injection, or documented support for alternative
   connections, to drop the `_updateStatus` hack. Small.

---

## 6. Risks and open points

* **Ordering of concurrent `runCell` calls.** `runCells` calls `runCell` for every cell concurrently
  (`actions.tsx:3365-3398`). The executor must allocate `seq` synchronously. Across clients, the
  `(seq, clientID)` tiebreak from the prototype gives a total order.
* **`stop_on_error` across a batch.** Today the kernel aborts the queued requests after an error.
  Now the proxy must skip the remaining entries **of the same batch**, so the intent needs a batch
  id and the executor must map `skipped` to "not run" without an error dialog.
* **Restarting cancels the queue.** `SessionContext.restartKernel` resolves after REST plus
  reconnect. The executor must resolve pending `runCell` promises as cancelled when `generation`
  changes. Otherwise "restart and run all" deadlocks.
* **Cell moves and undo** are delete+insert in the notebook Y doc (`actions.tsx:650-665`). The
  proxy must address cells by id and re-resolve on every write. The client no longer needs the 4.7
  future-reattach machinery.
* **Browser closed mid-run.** Nothing in the frontend depends on the original `runCell` promise
  surviving. A reopened client derives `[*]` from `execution_state` and progress from the `queue`.
  The execution indicator and `pendingInput` must therefore be derived from Y, not from in-memory
  state.
* **Double bandwidth in Phase 1** until the websocket filter exists.
* **Third-party extensions** calling `kernel.requestExecute` keep working through the real
  connection, or through the delegate in Phase 2. Their outputs never reach the Y doc, as today.
* **`CodeCell.onStateChanged` idle reaction** (`cells/src/widget.ts:1666-1669`) runs on every
  client. It is harmless only if the proxy writes `execution_count` and `execution_state`
  atomically.
* **Version floor:** `INotebookCellExecutor` needs ≥4.2. The `IKernelManager` plugin needs ≥4.4.
  The 4.7 undo machinery is irrelevant but harmless.
