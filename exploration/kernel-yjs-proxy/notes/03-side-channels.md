# 03 — Side channels: everything that is not "execute a cell and write outputs"

Scope: request/reply calls, stdin, comms and widgets, debugger, kernel status, display ids,
`clear_output(wait=True)`, payloads, consoles. For each one: how it works today, 1–3 ways to
carry it in the proxy architecture, and a recommendation.

Citation conventions:
- `msg.rst:N` = `/home/user/jupyter_client/docs/messaging.rst` (local copy of
  https://jupyter-client.readthedocs.io/en/latest/messaging.html), line N.
- `jl/...` = `/home/user/refs/jupyterlab/packages/...`
- `jc/...` = `/home/user/refs/jupyter-collaboration/...`
- `ipyw/...` = `/home/user/refs/ipywidgets/...`. Note: someone switched this shared clone to a
  sparse checkout while I was working. The `packages/...` paths I cite are read from git `HEAD`
  (`git -C /home/user/refs/ipywidgets show HEAD:<path>`).
- Prior art cloned for this note: `/home/user/refs/ypywidgets`, `/home/user/refs/yjs-widgets`,
  `/home/user/refs/ypywidgets-textual`.

---

## 0. Building blocks used throughout this note

The same few transport choices come up in every section, so they are defined once here.

| Name | What it is | Persisted? | Who receives it | Good for |
|---|---|---|---|---|
| **N-doc** | the notebook Y doc (`jupyter_ydoc` schema, `jl` RTC room) | yes (file + ystore) | all clients | outputs, execution counts, things that belong in the `.ipynb` |
| **K-doc** | a side Y doc per kernel session, e.g. room `kernel:<session-id>` | **no** (a `TransientRoom`, see below); its lifetime is the kernel session | all clients of that notebook | prompts, kernel_info, comm/widget registry, widget state, display index, queue |
| **Awareness** | y-protocols awareness on N-doc or K-doc | no, expires after about 30 s without a refresh | all clients | presence-like, small, last-value state (kernel status) |
| **RPC** | a unicast request/reply + server-push event channel to *one* client connection | no | only the requesting client (or a chosen set) | completions, inspect, history, is_complete, debugger, comm messages from frontend, stdin replies |
| **Y-RPC** | request/reply *written into* a Y map (`requests[id]`, `replies[id]`) in the K-doc | as K-doc | all clients | intents that must survive reconnects or be visible to everyone (interrupt, restart, stdin prompt) |

Existing building blocks we can reuse:

- **Transient (non-persisted) rooms already exist.** `jc/projects/jupyter-server-ydoc/jupyter_server_ydoc/rooms.py:553`
  (`TransientRoom`) is used for `JupyterLab:globalAwareness` (`handlers.py:152-163`). A K-doc can
  be a TransientRoom whose server-side peer is the proxy.
- **Unicast RPC on the existing room websocket already exists.** jupyter-collaboration adds a
  third message type next to SYNC/AWARENESS: `MessageType.RAW = 2`
  (`jc/projects/jupyter-server-ydoc/jupyter_server_ydoc/utils.py:26-30`). The server handles a
  `"save"` request with a correlation id and answers only on that websocket
  (`handlers.py:368-397`). The client side is `jc/packages/docprovider/src/yprovider.ts:43,183-240`.
  This is exactly the "per-client request/reply without polluting the doc" pattern we need.
  pycrdt-websocket also has an `on_message` filter hook that runs before SYNC/AWARENESS handling
  (`/home/user/refs/pycrdt-websocket/src/pycrdt/websocket/yroom.py:335-340`).
- **Persistence keeps everything by default.** `SQLiteYStore.squash_after_inactivity_of`
  defaults to `None` ("document history is never cleared",
  `jc/projects/jupyter-server-ydoc/jupyter_server_ydoc/stores.py:30-36`). So any value ever written
  to the N-doc stays in `.jupyter_ystore.db` unless squashing is configured. This matters for
  secrets (getpass) and for high-frequency noise (busy/idle, slider drags, progress bars).
- **Awareness entries without `user` are invisible in the collaborator UI.**
  `jc/packages/collaboration/src/collaboratorspanel.tsx:102-106` and `users-item.tsx:146-150` skip
  states that have no `user` field. The proxy can publish awareness without showing up as a
  phantom collaborator (or with a `user` field if we *want* a visible "Kernel" participant).

**Proposed rule of thumb (it carries through the whole note):**
> Durable state, and intents that must outlive a browser, go in Y (N-doc if it belongs in the
> file, K-doc otherwise). Interactive queries whose answer only the asker wants go over RPC.
> Presence-like status goes in awareness.

### 0.1 Where the RPC endpoint lives

- **Proxy runs inside jupyter-server** (like jupyter-server-nbmodel): the RAW handler on the room
  websocket calls the proxy directly. Same hop count as today (browser → server → ZMQ), so
  completion latency stays the same.
- **Proxy runs as a separate process that is a Y client**: the server has to relay RAW messages
  with an envelope `{conn_id, req_id, ...}` and route replies back by `conn_id`. That is more
  plumbing but still unicast. Alternative: a dedicated websocket endpoint
  `/api/kernelproxy/<session>/rpc` that the proxy itself serves.

### 0.2 Frontend compatibility shim (recommended in every section)

Most JupyterLab features (completer, tooltip, inspector, console, debugger, widget manager)
consume `Kernel.IKernelConnection` (`jl/services/src/kernel/kernel.ts:235-441`): `requestComplete`,
`requestInspect`, `requestHistory`, `requestIsComplete`, `requestCommInfo`, `requestDebug`,
`registerCommTarget`, `iopubMessage`, `statusChanged`, and so on. The cheapest way to get "no UI
change" is a **`ProxiedKernelConnection implements IKernelConnection`** that:
- maps `requestX` to RPC calls;
- exposes `status`/`statusChanged` from proxy awareness;
- exposes `iopubMessage`/`anyMessage` from an RPC event stream (for consumers such as the
  debugger and the console foreign handler);
- implements `requestExecute` by writing an execution intent into the doc (the execution
  topic, not this note);
- implements comms via the widget/comm mirror (§3).

Extensions then keep working unchanged. Today's `KernelConnection` stays as the fallback for
kernels opened without RTC.

---

## 1. Request/reply calls

### 1.1 Today

| Message | Spec | Used by in JupyterLab | Latency need |
|---|---|---|---|
| `complete_request` | `msg.rst:681` | `jl/completer/src/default/kernelprovider.ts:40-60`; providers are raced against a timeout (`jl/completer/src/manager.ts:62-67`) | **high**: Tab, and continuous hinting while typing |
| `inspect_request` | `msg.rst:613` | tooltip `jl/tooltip-extension/src/index.ts:355`; completer docs panel `kernelprovider.ts:113`; contextual help/inspector | medium–high (Shift+Tab, following the cursor) |
| `is_complete_request` | `msg.rst:806` | console Enter handling `jl/console/src/widget.ts:980-1000`, with a timer that falls back to "complete" on timeout; notebook `jl/notebook-extension/src/index.ts:2095` | high (on Enter) |
| `history_request` | `msg.rst:737` | console history `jl/console/src/history.ts:305`; notebook history `jl/notebook/src/history.ts:341`; inline completer `jl/completer/src/default/inlinehistoryprovider.ts:88,130` | low (inline completer: medium) |
| `kernel_info_request` | `msg.rst:914` | on every (re)connect `jl/services/src/kernel/default.ts:647`, queued specially at start/restart (`:462-475`); gives `language_info`, banner, help_links, `debugger: bool` | low; the result is almost static |
| `comm_info_request` | `msg.rst:885` | `jl/services/src/kernel/default.ts:992`; ipywidgets fallback restore `_loadFromKernelModels` (`ipyw/packages/base-manager/src/manager-base.ts:523`) | low |

Properties that matter for the design:
- The reply goes to the requester only: the shell ROUTER replies by ZMQ identity (`msg.rst:44`).
  In jupyter-server each websocket gets its own ZMQ session/identity.
- Every shell request produces `busy`/`idle` on iopub (`msg.rst:1375-1412`). So completions make
  the kernel status flicker for *everyone*, and that is broadcast today too.
- Shell requests queue behind a running execution. Completion is slow or unavailable while a cell
  runs (the same today).
- Payloads can be large: completion lists with `_jupyter_types_experimental` for `np.`/`pd.` can
  be tens to hundreds of KB, and inspect replies carry HTML/markdown bundles.

### 1.2 Options

**A. RPC over the room websocket (RAW-type message, or a dedicated rpc websocket).**
The client sends `{type:"kernel_rpc", id, msg_type, content}`. The proxy builds the Jupyter
message, sends it on shell, and sends the reply only to that connection.
- Latency matches today when the proxy is in-process (no Y encoding, no fan-out, no persistence).
- Nothing enters any Y history.
- Unicast by construction.
- The proxy can add value: **supersede stale requests** (keep at most one pending
  `complete_request` per client; when a newer one arrives, drop the older one if it has not been
  sent, or ignore its reply) and **cache `kernel_info`**.

**B. Y-RPC in an ephemeral K-doc** (`requests: Y.Map<id, {client, msg_type, content}>`,
`replies: Y.Map<id, ...>`, the proxy deletes both after delivery or a TTL).
- Works with "only Y" infrastructure, and a request survives a reconnect.
- But every keystroke completion is broadcast to *all* clients, which apply and discard it (N×
  fan-out of big payloads). It costs two extra Y round trips. Tombstones churn (GC handles that
  in a non-persisted doc). If the K-doc were persisted, every completion would end up in the
  ystore forever.

**C. Awareness** (the client puts `{completeRequest:{id,...}}` in its awareness state and the
proxy answers in its own).
- Rejected: awareness re-broadcasts the client's *whole* state on every change, to every client.
  It has no addressing and a 30 s expiry, and concurrent requests from one client overwrite each
  other. Fine for status, wrong for RPC.

**D. Keep the kernel websocket for these calls.**
- Works today with zero code. But it reintroduces the per-tab ZMQ session we are trying to
  remove, plus a second source of kernel status, duplicate iopub, and the risk that a tab sends
  `execute_request` directly.

### 1.3 Recommendation

- **A (RPC) for `complete`, `inspect`, `is_complete`, `history`, `comm_info`.** Wrap it behind the
  `ProxiedKernelConnection` shim (§0.2) so the completer, tooltip and console are unchanged.
- **`kernel_info`: published, not requested.** The proxy sends `kernel_info_request` at kernel
  start/restart and writes the reply into the K-doc (`kernel/info`). It is small and almost
  static, every client needs it, and it is needed even when the kernel is busy. The shim's
  `requestKernelInfo()` resolves from the K-doc. Optionally the proxy also keeps
  `metadata.language_info` in the N-doc up to date, which JupyterLab does client-side today
  (that is a document change, so it belongs to the proxy as kernel-owned state).
- The proxy may filter which `busy`/`idle` messages it reflects in the global status (see §5)
  so that typing does not make everyone's indicator blink.
- B stays available as a fallback only where a frontend can only speak Y. A non-JupyterLab
  client that only syncs Y could still post a request (e.g. a headless agent).

---

## 2. stdin: `input()` / `getpass()`

### 2.1 Today

- The kernel sends `input_request {prompt, password}` on the stdin channel and blocks until
  `input_reply {value}` arrives (`msg.rst:1453-1500`). It is only allowed if the
  `execute_request` had `allow_stdin=True`. Otherwise ipykernel raises `StdinNotImplementedError`
  immediately (`/home/user/ipykernel/ipykernel/kernelbase.py:1402-1406`).
- The `input_request` is routed to the frontend whose shell identity sent the execute request
  (`msg.rst:1501-1505`). In JupyterLab that means the tab that executed. Other collaborators see
  nothing.
- ipykernel flushes stale replies before each request, polls so that it can be interrupted
  (interrupt → `KeyboardInterrupt("Interrupted by user")`), and treats `"\x04"` as EOF
  (`kernelbase.py:1441-1491`).
- JupyterLab renders a `Stdin` widget in the output area (`jl/outputarea/src/widget.ts:572-650`),
  using `type=password` for getpass (`:1554-1563`). On Enter it calls `sendInputReply`
  (`:1344-1356`). It then **writes the echo into the model** as a `stream` output named `stdin`
  (`:616-628`), and with RTC that lands in the N-doc. The echoed text is `prompt + ' ' + value`
  (`:1295,1316-1317`); for passwords the value is replaced by a fixed `········` mask and is not
  pushed to input history (`:1351-1356`). So even today, only the mask reaches the shared model.
  Pending input clears on the `idle` status (`:859-864`).
- Prior art: jupyter-server-nbmodel stores a `PendingInput` server-side
  (`/home/user/refs/jupyter-server-nbmodel/jupyter_server_nbmodel/actions.py:296-321`) that the
  frontend discovers by polling the REST request status (`handlers.py:182`, `src/requestServer.ts:129-175`).
  So the prompt survives a reload, but it goes through REST rather than the doc.

### 2.2 New problems in the proxy world

1. **One identity.** The proxy owns the single shell+stdin socket pair, so *every*
   `input_request` comes to the proxy and the proxy decides who answers.
2. **Several collaborators.** Who may answer, and what happens with two answers?
3. **Nobody connected.** The kernel blocks indefinitely and the queue behind it stalls. The prompt
   must be visible when someone reopens the notebook.
4. **getpass secrecy.** The value must never enter a CRDT update. Y updates are broadcast to all
   peers and, by default, persisted forever in the ystore (§0). Deleting the value afterwards does
   not help: the insert stays in the update log. Even the *length* of the echo should not leak.

### 2.3 Designs

**S1. Prompt in Y, reply over RPC (recommended).**
- The proxy writes `stdin/pending = {request_id, cell_id, exec_id, prompt, password,
  requested_by, since}` in the K-doc (keyed by cell so the UI places it under the right output
  area). Because the K-doc lives as long as the kernel session, **reopening the notebook shows the
  prompt.**
- A client answers with RPC `{type:"input_reply", request_id, value}`. The value goes over a
  unicast socket to the proxy and then to ZMQ, and is never written to Y.
- The proxy is the arbiter: **the first valid reply for the current `request_id` wins.** The proxy
  sends `input_reply` and, in one transaction, deletes `stdin/pending` and appends the echo
  `{"output_type":"stream","name":"stdin","text": prompt + " " + (value or "········") + "\n"}` to the cell outputs in the
  N-doc. The echo is kernel-owned output, written by the proxy as today's frontend does. Late
  replies get an RPC error "already answered".
- Who may answer (policy, configurable):
  - (a) anyone;
  - (b) only `requested_by` (the user who queued the execution), with others seeing a read-only
    "waiting for input from Alice…" and an "answer instead" override;
  - (c) (b), but open to everyone once `requested_by` is no longer present in awareness.
  
  Recommend **(c)**: it matches today's semantics (the executor answers) and degrades gracefully.
- Cancel / interrupt: an interrupt makes the kernel raise `KeyboardInterrupt`. The proxy clears
  `stdin/pending` on `execute_reply` or `idle` for that parent, and on restart/dead. A "Cancel"
  button can send `"\x04"` (EOF → `EOFError`), which is gentler than an interrupt.
- Status: the proxy sets `execution_state` to a richer `"awaiting_input"` in its awareness/K-doc
  (§5), so collaborators see why the queue is stuck instead of a generic "busy".

**S2. Everything in Y** (the reply is written to `stdin/replies[request_id]`).
Simpler on the wire (no RPC needed), and Y's LWW gives a deterministic winner. But a reply must
be **forbidden for `password=True`** (secret in history and broadcast). Plaintext answers would
also be broadcast to every peer *before* the proxy echoes them, which is harmless for good faith,
but it means two code paths. Only acceptable as a fallback for Y-only clients and non-password
prompts.

**S3. Avoid blocking when nobody is there.**
- When the proxy sends `execute_request` and no human client is present (awareness empty), it can
  set `allow_stdin=False`. `input()` then fails fast with `StdinNotImplementedError` instead of
  hanging a "run all, close laptop" session.
- Or keep `allow_stdin=True` and add an optional `stdin_timeout` after which the proxy replies EOF.
- Both are policies, **off by default**: reopening the browser and answering a waiting prompt is a
  feature of this architecture.

**Secrecy checklist for getpass:**
- The value travels only over RPC (TLS websocket) and ZMQ.
- The echo is a fixed mask, not one bullet per character.
- Do not add password values to the client-side input history.
- Never log RPC payloads of type `input_reply` with `password=True` in the proxy or server debug
  logs: `pycrdt-websocket` logs message types at debug level, so the RPC path needs similar care.
- The *prompt* text is not secret and can live in the K-doc.

### 2.4 Recommendation

**S1**, using policy (c), with the S3 options available but off by default. Open question for the
user: **should a reply from a different user than the executor be allowed at all** (data leakage /
"who typed the password" auditing)? Should the K-doc record `answered_by`?

---

## 3. Comms and ipywidgets

### 3.1 Today

**Comm protocol** (`msg.rst:1522-1595`):
- `comm_open`, `comm_msg` and `comm_close` go both ways.
- Kernel→frontend messages are broadcast on iopub to *all* clients. Frontend→kernel messages go on
  shell from *one* client.
- A comm handler publishes busy/idle with the comm message as parent (`msg.rst:1588-1593`), so
  output produced in a widget callback has `parent = comm_msg`.
- `handleComms` must be true on exactly one `KernelConnection` per tab
  (`jl/services/src/kernel/default.ts:62,297`).

**ipywidgets protocol v2** (`ipyw/packages/schema/messages.md:200-365`):
- Opening: `comm_open` on target `jupyter.widget` carries the full `state` plus `buffer_paths`;
  binary buffers can sit at any nested path (`messages.md:217,250-269`).
- Updates: `update {state, buffer_paths}` in both directions (`:273-290`).
- Since 2.1: the kernel sends `echo_update` to rebroadcast frontend-originated changes to all
  frontends (`:292-317`). On the frontend: `ipyw/packages/base/src/widget.ts:264-293`; on the
  kernel: `python/ipywidgets/ipywidgets/widgets/widget.py:630-645`, enabled by default in 8.x
  via `JUPYTER_WIDGETS_ECHO` (`widget.py:43`).
- Other messages: `request_state` (`:319-332`); `custom` for events such as Button clicks
  (`:334-348`). Display is a `display_data` with `application/vnd.jupyter.widget-view+json`
  carrying `model_id` (`:350-364`).
- Control comm `jupyter.widget.control`: `request_states` gets every widget's state in one
  message (`:366-420`).

**Frontend manager:**
- `@jupyter-widgets/base-manager` `_loadFromKernel` opens the control comm and calls
  `request_states`, falling back to `comm_info` + per-model `request_state`
  (`manager-base.ts:399-448,523`).
- JupyterLab's manager (`python/jupyterlab_widgets/src/manager.ts`) registers the comm target on
  kernel change (`:78-109`) and restores widgets from the kernel or from notebook metadata
  (`:380-383,494-503`).
- On save it can write `metadata.widgets["application/vnd.jupyter.widget-state+json"]`
  (`:446-466`), but `saveState` defaults to `false` (`plugin.ts:90`).
- Throttling: a model keeps at most 1 pending update and waits for the iopub `idle` of its own
  `comm_msg` before sending the buffered latest state (`ipyw/packages/base/src/widget.ts:370-390,517,624`).

**Output widget:**
- The kernel side sets `msg_id` to the current request's msg_id inside `with out:`
  (`python/ipywidgets/ipywidgets/widgets/widget_output.py:107-141`). The `outputs` trait is "The
  output messages synced from the frontend" (`:60-61`).
- The *frontend* registers a message hook for that msg_id
  (`python/jupyterlab_widgets/src/output.ts:65-79`). The hook swallows the iopub messages so they
  do not reach the cell (`:31-33`), adds them to its own `OutputAreaModel`, and **saves `outputs`
  back to the kernel** (`:81-101`).
- So capture is client-side. With 2 tabs open both capture and both write back. With 0 tabs open
  nothing is captured, and output printed into an Output widget while the browser is closed is
  lost.

**What survives a browser close today:** widget *models* survive in the kernel and are restored
via `request_states`. What is lost: Output-widget captures made while nobody was connected, and
anything held only in frontend views.

### 3.2 Prior art

- **ypywidgets + yjs-widgets** (QuantStack, David Brochart): `/home/user/refs/ypywidgets`,
  `/home/user/refs/yjs-widgets`.
  - Each widget *is* a pycrdt `Doc` in the kernel, with `_attrs: Map` and `_model_name: Text`
    (`ypywidgets/src/ypywidgets/widget.py:6-26`).
  - It syncs with the frontend by tunnelling y-protocol SYNC/AWARENESS messages in comm buffers on
    target `ywidget` (`ypywidgets/src/ypywidgets/comm.py:20-79`).
  - It renders through a new mimetype `application/vnd.jupyter.ywidget-view+json` (`comm.py:110-115`)
    in `yjs-widgets/src/notebookrenderer/widgetManager.ts:53`, with
    `yjs-widgets/src/notebookrenderer/yCommProvider.ts:32-88`.
  - Lesson: CRDT widget state works, and the kernel can be a Y peer. But it is *not*
    ipywidgets-compatible (new widget libraries are needed).
  - In our architecture a ypywidget is trivially supported: the proxy relays y-sync messages
    between the `ywidget` comm and a subdoc/map in the K-doc, and the proxy becomes the kernel
    doc's Y peer. No translation is needed.
- `trungleduc/ywidgets` (2021, "Yjs wrapper widget for ipywidgets"), `QuantStack/r-ywidgets`
  (Yrs-based, for R), `davidbrochart/ypywidgets-textual` (TUI views over the same Y model, which
  shows the "views anywhere" idea). Found via `gh search repos ywidgets`. There is no
  "jupyter-ywidgets" or "ipywidgets yjs" repo beyond these.
- jupyter-server-nbmodel does not handle comms or widgets at all.

### 3.3 Designs

**W0. Transparent comm relay (baseline, for any comm target).**
- The proxy forwards kernel comm traffic (iopub) to connected clients over the RPC event stream,
  and client comm traffic (RPC) to shell.
- The proxy keeps a `comms` registry `{comm_id: {target_name, opened_at}}` in the K-doc. It
  answers `comm_info_request` from it and deletes entries on `comm_close`.
- This is equivalent to today, including the losses (nothing is persisted, and late joiners only
  get state if the protocol supports re-sync). It is required for non-widget comm users (Bokeh
  `push_notebook`, custom extensions, the debugger-adjacent variable explorers that use comms) and
  for any widget library the proxy does not mirror.

**W1. Proxy is the widget "server-side manager"; widget state is mirrored into the K-doc
(recommended).**

*Kernel side: unchanged ipywidgets.*

*Proxy:*
- Treats `jupyter.widget` and `jupyter.widget.control` as known targets and keeps a Y map per model
  in the K-doc:
  ```
  widgets: Y.Map<model_id, Y.Map{
      _model_module, _model_name, _model_module_version, _view_module, _view_name, _view_module_version,
      state:   Y.Map<attr, JSON>        # LWW per attribute; atomic values (ipywidgets semantics)
      buffers: Y.Map<pathJSON, Uint8Array>  # Yjs stores binary natively (ContentBinary)
      outputs: Y.Array<output>           # only for OutputModel, see below
  }>
  ```
  Keep values as JSON leaves, not nested Y types. Widget attributes are atomic for the kernel,
  and nested CRDT merges would produce states the kernel never validated. `IPY_MODEL_<id>`
  references (children, layout, style) stay as strings.
- **Kernel → Y.** `comm_open` creates the entry (the full state comes with it). `update` and
  `echo_update` are applied in a transaction with origin `"kernel"`. `comm_close` deletes the
  entry.
- **Y → kernel.** The proxy observes changes whose origin is not its own and sends `comm_msg
  {method:"update", state:{changed}, buffer_paths}`. It **throttles in the same way as
  ipywidgets**: at most one in-flight `update` per model, waiting for the `idle` that has that
  comm_msg as parent, and coalescing everything else into the latest value. This also absorbs
  slider-drag storms from N users.
- **Conflicts.** The kernel is the authority. The proxy forwards client writes in the order it
  observes them. The kernel's `echo_update` (default on in 8.x) and any validator-corrected
  `update` come back in that same order and are written by the proxy, so Y converges to the
  kernel's value. A transient LWW "win" by a client over a concurrent proxy write is forwarded to
  the kernel and then echoed back, so it converges either way. Recommend keeping
  `JUPYTER_WIDGETS_ECHO` on. With ipywidgets 7 (no echo), the proxy synthesizes the echo itself:
  it treats the forwarded value as accepted unless the kernel sends a corrective `update`.
- **Custom messages** (`method:"custom"`) are *events*, not state. Kernel→frontend: broadcast to
  connected clients over the RPC event stream, and lost if nobody is connected (as today).
  Frontend→kernel (Button `click`, etc.): RPC → proxy → `comm_msg`. Never in Y.
- **Restore without the kernel.** Clients rebuild every model from the K-doc on open, with no
  `request_states` round trip. If the *proxy* restarts while the kernel lives, it rebuilds the
  K-doc from the kernel with `request_states` (control comm, full state + buffers).
- **Notebook metadata.** Do not live-mirror widget state into the N-doc `metadata.widgets`: every
  drag would be persisted to the ystore. If the `saveState` setting is on, the proxy (or a save
  hook) snapshots the K-doc into `metadata.widgets` at save time only, equivalent to
  `manager.ts:456-466`, so static renderers (nbviewer, Voilà-static) keep working.
- **Lifetime.** The K-doc widget maps live as long as the kernel session. They are cleared on
  restart or shutdown (the kernel-side objects are gone, so persisting them would show dead
  views).

*Frontend:*
- Implement a `YWidgetManager` (subclass of `ManagerBase` / `LabWidgetManager`) whose
  `_create_comm` returns a **shim `IClassicComm` per model backed by the K-doc map**:
  - `comm.send({method:"update"})` → write to `state`/`buffers`;
  - `comm.send({method:"custom"})` → RPC;
  - Y changes from origin ≠ self → synthesized `{method:"update"}` delivered to the model's
    `_handle_comm_msg`.
- Because it is implemented at the comm layer, **every existing widget JS library (bqplot,
  ipyleaflet, ipydatagrid, plotly FigureWidget, ipyvolume…) works unchanged.** Their JS still runs
  in the browser; only the transport changes.
- The shim must fake the `idle` status callback that `WidgetModel` throttling expects
  (`widget.ts:370-390`). The simplest fix: call it immediately after the Y write, because the
  proxy does the real throttling.

**W2. Kernel-side Y (ypywidgets style), proxy as pure Y relay.**
The long-term "cleanest" option: widget state is born as a CRDT in the kernel, and the proxy only
bridges y-sync between the comm and the K-doc. But it needs every widget library to be
rewritten, so it is not viable for the ecosystem in the short term. Support it opportunistically
as a passthrough (see §3.2).

### 3.4 Output widget: routing by msg_id in the proxy

This is where W1 is clearly better than today:
- The proxy sees `OutputModel.state.msg_id` change (a kernel `update`) and pushes
  `route[msg_id] → model_id` onto a **stack** (LIFO, like JupyterLab message hooks, which run
  newest first). When `msg_id` is reset to `''`, it pops the route.
- For each iopub message with `parent.msg_id = P`:
  1. If `route[P]` exists, append the output to `widgets[model_id].outputs` (also handling
     `clear_output(wait)` there, §5.4) and **do not** write it to the cell. Then forward
     `update {outputs:[...]}` to the kernel so the kernel's `outputs` trait matches
     (`widget_output.py:61` expects the frontend to do this; now the proxy does it, exactly once
     instead of N times).
  2. Else apply the display-id index (§5.3).
  3. Else write to the cell that owns execution `P` (execution topic).
- This covers:
  - `with out:` inside a cell (`P` = the execute_request);
  - `@out.capture()` in widget callbacks (`P` = a `comm_msg` the proxy itself sent, so the proxy
    knows it);
  - background threads that print into `out` after the cell finished (the route lives until
    `msg_id` is reset, independent of the cell's execution status).
- Output arriving while no browser is open is captured and visible on reopen.
- Kernel-side `out.append_stdout()` / `append_display_data()` set the `outputs` trait from the
  kernel and flow as normal `update`s.
- Volume: Output-widget outputs live in the K-doc, so the same stream-coalescing and trimming
  rules as cell outputs apply (another note covers that).

### 3.5 Which "common widgets" work purely server-side?

With W1, *state* for all of them lives server-side and survives close/reopen. The question is
which behaviours need browser JS to *run*:

| Widget / feature | Server-side only? | Notes |
|---|---|---|
| IntSlider, FloatSlider, Dropdown, Text, Checkbox, ToggleButtons, Select*, HTML, Label, Image, Box/HBox/VBox/Tab/Accordion, Layout/Style | **yes** | pure state; views render from the K-doc |
| IntProgress / FloatProgress, `tqdm.notebook` (HBox of HTML+FloatProgress+HTML) | **yes** | high-frequency `update`s; the proxy coalesces Y writes per attribute (e.g. ≤10–20 Hz, last value wins). This is safe because the state is LWW. Fixes the "progress bar while browser closed" case |
| Output widget | **yes, via proxy routing** (§3.4) | better than today |
| Button | partly | the click is a `custom` *event* from a browser. It works while someone is connected, and there is nothing to persist |
| Play | **no** | the timer runs in frontend JS. With 0 browsers it stops; with N browsers each one advances `value` (a duplicate-advance bug that also exists today with 2 tabs). Could be special-cased in the proxy (the proxy runs the Play timer) |
| jslink / jsdlink (`LinkModel`) | **no** | runs in frontend JS. Idempotent with N frontends, inert with 0. Could also be special-cased in the proxy, since it is a simple attribute copy |
| FileUpload | needs a browser to pick the file | `value` with buffers then goes into the K-doc. Large buffers are a reason to keep the K-doc non-persisted (see below) |
| Controller (gamepad), media capture widgets | no | browser-only by nature |
| Custom JS widgets (bqplot, ipyleaflet, ipycanvas…) | state yes, behaviour no | JS-computed state (e.g. map bounds after a resize, canvas image data) only updates while a browser is open. ipycanvas draw commands are `custom` messages, so they are lost while closed (as today) unless the library syncs an image back |

**Binary buffers:**
- Store them as `Uint8Array` values in the per-model `buffers` map, keyed by the JSON-encoded path,
  and reassemble `{state, buffer_paths, buffers}` in the shim.
- Each update of a 10 MB array creates a new 10 MB Y item. With GC on, the old one is freed in
  memory, but a *persisted* update log would grow without bound. That is one more reason the
  widget K-doc must be a TransientRoom (in-memory only), with an optional save-time snapshot.
- Very large buffers could instead be stored as content-addressed blobs served over HTTP, with
  the Y map holding the hash. Phase 2.

### 3.6 Recommendation

**W1** for `jupyter.widget` / `jupyter.widget.control` (the proxy is the single widget client of
the kernel and mirrors state into a non-persisted K-doc; the frontend uses a Y-backed comm shim),
plus **W0** relay for every other comm target, plus **W2 passthrough** for `ywidget`.
Special-casing Play and jslink in the proxy is optional later.

---

## 4. Debugger

### 4.1 Today

- `debug_request` / `debug_reply` on the **control** channel carry DAP Request/Response
  (`msg.rst:1085-1103`). Jupyter adds `dumpCell`, `debugInfo`, `inspectVariables`,
  `richInspectVariables`, `copyToGlobals` (`msg.rst:1104-1163`).
- DAP events arrive as `debug_event` on iopub, broadcast (`msg.rst:1437-1451`).
- Frontend: `jl/debugger/src/session.ts:82,301` (`kernel.requestDebug`), with events from
  `iopubMessage` filtered to `debug_event` (`session.ts:78,274-283`). The kernel advertises support
  via `kernel_info_reply.debugger`.
- State restore after reload already exists through `debugInfo` (breakpoints, stopped threads,
  etc.), which the kernel holds.
- While stopped at a breakpoint the shell is blocked (queued executions and completions wait),
  but the control channel keeps answering.

### 4.2 Options

- **D1. Keep a direct websocket** (`/api/kernels/<id>/channels`), opened lazily only when the
  user enables the debugger.
  - Pros: zero porting. The debugger is per-user and interactive, and has no persistence value.
  - Cons: a second ZMQ session on the kernel duplicates iopub (that tab would also see all
    execution traffic and a second status stream). It must be prevented from executing (wrap it
    with `handleComms=false` and a guard). It is also a second auth path to the kernel.
- **D2. Tunnel over RPC.** `debug_request` is plain request/reply through the proxy's control
  socket, and `debug_event` is pushed over the RPC event stream to clients that subscribed
  (debugger panel open).
  - The proxy must **remap DAP `seq`/`request_seq` per client**: several clients share the
    proxy's single control session, and the kernel echoes `request_seq`, so concurrent clients
    would collide.
  - With `ProxiedKernelConnection.requestDebug` and `iopubMessage` (filtered to `debug_event`),
    `@jupyterlab/debugger` works unmodified.
- **D3. Debugger state in Y** (breakpoints shared in the K-doc, a "stopped at" indicator for all
  collaborators). This is a feature *on top of* D2, not instead of it. Collaborative debugging
  needs product decisions (who steps?).

### 4.3 Recommendation

**D2 (tunnel)**, implemented as a thin RPC passthrough with seq remapping. Justification: the
traffic is request/reply plus broadcast events, which is exactly what the RPC layer already does
for completions. A raw kernel websocket would bring back everything the architecture removes
(per-tab ZMQ sessions, duplicate iopub, a second status source, a back door around the execution
queue).

**D1 is an acceptable stopgap** if D2 slips: the debugger is the one feature where a direct
connection is defensible, because it is single-user and interactive and its state lives in the
kernel. If D1 is used, open the socket only while the debugger is on, and ignore its non-debug
iopub.

---

## 5. Kernel status, display ids, clear_output, transient, payloads

### 5.1 Kernel status / busy–idle

**Today:**
- iopub `status` (`starting|busy|idle`) is sent around *every* request, including completions and
  comm messages (`msg.rst:1375-1412`).
- `KernelConnection._updateStatus` adds the connection-level states `restarting`, `dead`,
  `autorestarting` and `unknown` (`jl/services/src/kernel/default.ts:551,634,1272-1293,1487`).
- Cells have `execution_state` in the shared model (`/home/user/refs/jupyter_ydoc/jupyter_ydoc/ynotebook.py:49`,
  `jupyter_ydoc/javascript/src/ycell.ts:787-793`); the execution topic covers it.

**Options:**
- **Awareness of the proxy client** (on the N-doc, or on the K-doc if every client joins it):
  `{kernel:{id, name, status, execution_state, connection, last_activity}}` with no `user` field,
  so it stays hidden from the collaborator UI (§0).
  - Ephemeral, broadcast and small, which is exactly awareness's job.
  - **Liveness comes for free**: if the proxy dies, its awareness entry expires (about 30 s) and
    clients show "unknown/disconnected".
- **K-doc map field** (`kernel/status`). Equally good if the K-doc is non-persisted. It also lets
  late joiners read it, and so does awareness.
- N-doc field: rejected (busy/idle flips would be persisted forever).

**Recommendation:** awareness for live status, plus a K-doc field for slower-changing facts
(`kernel_info`, kernel id/name, `started_at`, restart count).
- The proxy **debounces** (e.g. ignore busy→idle within 100 ms) and can **reflect only
  execute/comm parents** in the global indicator. Completions do not make everyone's dot blink.
  This is a deliberate UX change, so it needs to be configurable.
- Add the derived states `awaiting_input` (§2) and `debugging/stopped` (§4).

### 5.2 Interrupt / restart / shutdown (brief)

These are *intents* that should work from any client and be visible to all:
- Y-RPC in the K-doc (`commands: Y.Map<id, {type:"interrupt"|"restart", by, at}>`). The proxy
  executes it, marks it done, and garbage-collects it.
- Or plain RPC with the proxy broadcasting the result.

Interrupt is idempotent and harmless with LWW. Restart must also clear K-doc state (widgets,
display index, stdin prompt, comm registry) and reset per-cell `execution_state`.

### 5.3 `display_data` with `display_id`, `update_display_data`

**Spec:**
- `transient.display_id` (`msg.rst:1219-1258`) must not be persisted to documents (`:1252`).
- `update_display_data` updates *all* displays with that id, even if there are several
  (`:1269-1302`). In practice that means across cells and across executions.

**JupyterLab today has two layers:**
- `KernelConnection._handleDisplayId` keeps `displayIdToParentIds` and re-routes a
  `display_data` whose id has been seen before as `update_display_data` to every future that
  displayed it (`jl/services/src/kernel/default.ts:1190-1240`, called from `:1615-1628`).
- `OutputArea` keeps `_displayIdMap: display_id → [output indices]` per area
  (`jl/outputarea/src/widget.ts:830-876,938`). `notebook/src/actions.tsx:2779-2800,2948-2987`
  saves and restores it.
- Mappings are dropped when the future is disposed (`default.ts:415-445`). Notebook futures use
  `disposeOnDone=false` (`outputarea/src/widget.ts:1074`), so they stay alive until the cell
  re-executes.
- **A page reload loses all of it**, so after a reload `update_display_data` for an earlier cell
  silently does nothing.
- jupyter-server-nbmodel leaves both as `FIXME`
  (`/home/user/refs/jupyter-server-nbmodel/jupyter_server_nbmodel/actions.py:285-293`).

**Proxy design:**
- The proxy keeps `display_index: display_id → [OutputRef]`, where `OutputRef` points at a
  **specific output Y.Map** (in a cell's outputs, or in an Output widget's `outputs`). Hold Y
  object references rather than `(cell, index)`: indices shift when outputs are added, cleared or
  trimmed, and cells move. Prune refs whose Y item is deleted (cell cleared, re-executed or
  deleted, output trimmed).
- `display_data` with id:
  - if the id is known, update every existing ref (as `default.ts:1197-1211` does);
  - in all cases, append the new output and add a ref.
- `update_display_data` (no append): for each ref, replace `data`/`metadata` in one transaction
  with origin `"kernel"`. Coalesce bursts per ref (a display-handle progress bar updated in a
  loop), keeping the last value.
- **Index rebuild.** So that the index survives a proxy restart, store the id in the output map
  under a reserved key (e.g. `transient: {display_id}`), and strip it when serializing to
  `.ipynb`, as the spec requires. `jupyter_ydoc` serialization would need that one-line strip. The
  alternative is to keep the index only in proxy memory and lose cross-cell updates after a proxy
  restart (this is the situation today after a reload).
- **Scope.** Entries are scoped to the kernel session: cleared on restart; earlier executions'
  displays stay targetable until their outputs are removed. This is strictly better than today
  (it works after reopening the browser and across collaborators).

### 5.4 `clear_output(wait=True)`

**Today:** `OutputAreaModel.clear(wait)` sets `clearNext` and clears on the next `add`
(`jl/outputarea/src/model.ts:279-307,409`). If no further output arrives, the old outputs stay.
The Output widget does the same (`python/jupyterlab_widgets/src/output.ts:93-105`).
jupyter-server-nbmodel ignores `wait` (`actions.py:285-290`).

**Proxy:**
- Keep `pending_clear[target]` in memory, where the target is a cell or an Output widget.
- On the next output for that target, do delete-all plus insert-new **in a single Y transaction**.
  Peers then see the swap atomically (no flicker), and the update log records one compound change.
- Reset the flag when a new execution of that cell starts. The cell is cleared at start anyway.
- This is also a natural **trimming point**: animation loops (`clear_output(wait=True)` +
  `display`) churn delete/insert pairs, which a later GC/squash pass can collapse. See the volume
  note.

### 5.5 `transient`

Never write it to the N-doc as content, except the reserved `display_id` key above, which is
stripped on save. Other future transient keys pass through in the RPC event stream only.

### 5.6 Payloads in `execute_reply` (deprecated but alive, `msg.rst:542-612`)

- **`page`** (IPython `?`/`??`):
  - Today: the pager handler (`jl/outputarea/src/tokens.ts:16-25`) or a fallback that converts it
    to a `display_data` output (`jl/outputarea/src/widget.ts:885-911`).
  - Proxy: write it as a `display_data` output in the N-doc, which is today's default and survives
    reopen. Optionally tag it `metadata: {"jupyter": {"source": "page"}}` so a frontend with a
    pager handler can show it in a side panel instead.
- **`set_next_input`** (`%load`, `%recall`, `obj??` in some setups):
  - Today the *frontend* that executed does it: `replace` → `setSource`, else insert a new code
    cell below (`jl/notebook/src/cellexecutor.ts:156-190`); for the console:
    `jl/console/src/cellexecutor.ts:27-30`.
  - Proxy options: (a) the proxy applies it to the N-doc (works with the browser closed). This is
    the one case where the proxy edits *user-owned* content (source / cell list), so it needs an
    explicit exception to the "proxy only touches kernel-owned state" rule. (b) The proxy
    publishes it as an event in the K-doc, the requesting client applies it, and the proxy
    applies it if that client is gone.
  - Recommend **(a)**, with origin `"kernel"` so it is not captured in users' undo stacks. Keep
    the JupyterLab rule of not marking the new cell trusted (`cellexecutor.ts:180-185`).
- **`edit`**: ignored (as in JupyterLab today).
- **`ask_exit`**: console only; becomes a console-doc event.

---

## 6. Consoles

**Today:**
- A console is *not* a document. `CodeConsole` holds local cell models and executes over its own
  session (`jl/console/src/widget.ts`). It can attach to a notebook's kernel ("New Console for
  Notebook").
- The `ForeignHandler` (`jl/console/src/foreign.ts:81-140`) shows executions from *other*
  sessions on the same kernel by comparing `parent_header.session` with its own `clientId`.
  Console history comes from `history_request` (`jl/console/src/history.ts:305`).

**With the proxy:**
- If the console shares the notebook's kernel, its executions go through the same proxy (the
  proxy owns the kernel's only client session). The proxy has to route console-originated
  outputs to the console, not to a cell. The "foreign" feature becomes "show everything the
  proxy executed", and the proxy knows the origin of each request.
- **Option C1: a console Y doc** (K-doc-like, `console:<id>`):
  - a `cells` log of `{input, outputs, execution_count, by}` plus a shared or per-user prompt
    cell, with execution intents as in the notebook;
  - shared REPL for collaborators and reload-safe, for free;
  - non-persisted (TransientRoom) or persisted with a TTL, and trimmed aggressively (consoles are
    logs);
  - `is_complete` stays RPC, keeping the console's timeout fallback (`widget.ts:980-1000`).
- **Option C2:** consoles keep using a direct kernel websocket. That has the same drawbacks as D1,
  plus duplicated iopub when sharing a notebook kernel.
- **Recommend C1, later:** reuse the notebook execution machinery against a log-shaped doc. Until
  then, a console on a proxied kernel can use `ProxiedKernelConnection` with a "console target"
  routing tag.

---

## 7. Summary of recommendations

| Concern | Transport | Persisted |
|---|---|---|
| complete / inspect / is_complete / history / comm_info | RPC (RAW message on room websocket), behind a `ProxiedKernelConnection` shim; proxy drops superseded completions | no |
| kernel_info | proxy publishes it to the K-doc at start/restart | session |
| stdin prompt | K-doc `stdin/pending` (visible to all, survives reopen) | session |
| stdin reply (incl. getpass) | RPC only; proxy arbitrates first reply; proxy writes the masked echo to the N-doc | echo only |
| widget state (ipywidgets) | proxy = widget client; K-doc maps (JSON leaves + binary buffers); frontend Y-backed comm shim | session (+ optional snapshot on save) |
| widget custom messages, other comms | RPC event stream / relay | no |
| Output widget capture | proxy routes iopub by `msg_id` stack into the widget's K-doc outputs | session |
| debugger | RPC tunnel with DAP seq remapping (direct websocket as a stopgap) | no |
| kernel status | proxy awareness (no `user` field), debounced; derived `awaiting_input` / `stopped` | no |
| interrupt/restart | K-doc command map or RPC | no |
| display_id | proxy index of Y output refs, plus reserved `transient.display_id` key stripped on save | N-doc |
| clear_output(wait) | proxy pending flag; atomic swap transaction | N-doc |
| page / set_next_input | proxy writes them to the N-doc (set_next_input = explicit exception to ownership rule) | N-doc |
| console | later: console Y doc reusing execution intents | TTL / none |

## 8. Open questions for the user

1. **RPC on the room websocket** (a new RAW subtype next to `save`) or a **dedicated proxy
   websocket endpoint**? This depends on whether the proxy runs in-process with jupyter-server.
2. **Is a K-doc acceptable as a non-persisted TransientRoom** whose lifetime is the kernel
   session? That rules out "reopen after server restart shows widgets/prompt"; the kernel is gone
   then anyway.
3. **stdin answer policy:** executor-only, executor-then-anyone (proposed), or anyone? Should
   `allow_stdin=False` apply when nobody is connected?
4. **Status UX:** may the proxy hide busy/idle caused by completions from the global indicator?
5. **set_next_input:** OK for the proxy to edit source / insert cells?
6. **Widgets:** is a frontend comm-shim manager acceptable (all JS widgets keep working), or should
   we also push W2 (Y-native widgets) as the long-term direction? Should the proxy emulate Play and
   jslink server-side?
7. **display_id persistence:** allow a reserved `transient` key in Y output maps (stripped on
   save) so the index can be rebuilt?
8. **Debugger:** is D1 (direct websocket while debugging) acceptable as phase 1?
