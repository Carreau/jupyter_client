import asyncio
import statistics
import time

import pytest
from pycrdt import Doc

from kyproxy.frontend import Frontend, wait_until
from kyproxy.proxy import KernelProxy
from kyproxy.schema import sort_key
from kyproxy.transport import RoomServer

NB, KR = "/nb/test.ipynb", "/kernel/test.ipynb"


@pytest.fixture
async def server():
    async with RoomServer() as s:
        yield s


@pytest.fixture
async def proxy(server, request):
    kw = getattr(request, "param", {})
    async with KernelProxy(server.port, NB, KR, **kw) as p:
        yield p


@pytest.fixture
async def fe(server, proxy):
    async with Frontend(server.port, NB, KR) as f:
        await wait_until(lambda: f.k.status.get("execution_state") == "idle")
        yield f


def kernel_list(f, cid):
    return f.outputs_text(cid).strip()


async def test_run_one_cell(fe):
    cid = fe.add_cell("print('hello')\n1+1")
    [r] = fe.execute(cid)
    await fe.wait_done(r)
    c = fe.cell(cid)
    assert fe.req(r)["state"] == "ok"
    assert c["execution_count"] == 1
    assert c["execution_state"] == "idle"
    assert [o["output_type"] for o in c["outputs"]] == ["stream", "execute_result"]
    assert c["outputs"][0]["text"] == "hello\n"
    assert c["outputs"][1]["data"]["text/plain"] == "2"


async def test_two_clients_concurrent_queue_is_deterministic(server, fe):
    async with Frontend(server.port, NB, KR) as fb:
        setup = fe.add_cell("import time\norder = []\ntime.sleep(1.0)")
        a = [fe.add_cell(f"order.append('A{i}')") for i in range(3)]
        b = [fb.add_cell(f"order.append('B{i}')") for i in range(3)]
        await wait_until(lambda: len(fe.cell_ids()) == 7 and len(fb.cell_ids()) == 7)
        [rs] = fe.execute(setup)
        await wait_until(lambda: fb.req(rs).get("state") == "running")
        # Both clients enqueue "at the same time" (no sync in between)
        ra = fe.execute(*a)
        rb = fb.execute(*b)
        await fe.wait_done(rs, *ra, *rb)
        recs = sorted((fe.req(r) for r in ra + rb), key=sort_key)
        expected = [fe.cell(r["cell_id"])["source"].split("'")[1] for r in recs]
        check = fe.add_cell("print(order)")
        [rc] = fe.execute(check)
        await fe.wait_done(rc)
        got = eval(kernel_list(fe, check))
        assert got == expected
        # same seq numbers were chosen concurrently -> client id broke the tie
        assert fe.req(ra[0])["seq"] == fb.req(rb[0])["seq"]


async def test_run_all_preserves_document_order(fe):
    fe.add_cell("order = []")
    for i in range(5):
        fe.add_cell(f"order.append({i})")
    fe.add_cell("print(order)")
    rids = fe.run_all()
    await fe.wait_done(*rids)
    last = fe.cell_ids()[-1]
    assert kernel_list(fe, last) == "[0, 1, 2, 3, 4]"
    assert [fe.cell(c)["execution_count"] for c in fe.cell_ids()] == list(range(1, 8))


async def test_cell_moved_while_queued_and_while_running(fe):
    c0 = fe.add_cell("import time\nprint('a', flush=True)\ntime.sleep(0.6)\nprint('b')")
    c1 = fe.add_cell("print('queued one')")
    r0, r1 = fe.execute(c0, c1)
    await wait_until(lambda: "a" in fe.outputs_text(c0))
    # move the running cell (delete + re-insert clone, like JupyterLab) ...
    fe.move_cell(0, 1)
    # ... and the queued one
    fe.move_cell(0, 1)
    await fe.wait_done(r0, r1)
    await asyncio.sleep(0.2)
    assert fe.outputs_text(c0) == "a\nb\n"
    assert fe.outputs_text(c1) == "queued one\n"
    assert len(fe.cell_ids()) == 2


async def test_client_disconnect_mid_execution(server, fe):
    async with Frontend(server.port, NB, KR) as fa:
        cid = fa.add_cell("import time\nfor i in range(5):\n    print(i, flush=True)\n    time.sleep(0.2)\n'done'")
        [r] = fa.execute(cid)
        await wait_until(lambda: fa.req(r).get("state") == "running")
    # fa is gone (browser closed). fe stayed connected but let's also use a brand new client:
    del fe
    await asyncio.sleep(1.5)
    async with Frontend(server.port, NB, KR) as fresh:
        await fresh.wait_done(r)
        c = fresh.cell(cid)
        assert c["outputs"][0]["text"] == "0\n1\n2\n3\n4\n"
        assert c["outputs"][1]["data"]["text/plain"] == "'done'"
        assert c["execution_count"] == 1


async def test_error_cancels_queued(fe):
    c0 = fe.add_cell("1/0")
    c1 = fe.add_cell("print('never')")
    r0, r1 = fe.execute(c0, c1)
    await fe.wait_done(r0, r1)
    assert fe.req(r0)["state"] == "error"
    assert fe.req(r1)["state"] == "cancelled"
    assert fe.cell(c0)["outputs"][0]["ename"] == "ZeroDivisionError"
    assert fe.cell(c1)["outputs"] == []


async def test_interrupt(fe):
    c0 = fe.add_cell("import time\ntime.sleep(30)")
    c1 = fe.add_cell("print('after')")
    r0, r1 = fe.execute(c0, c1)
    await wait_until(lambda: fe.req(r0).get("state") == "running")
    await asyncio.sleep(0.3)
    tok = fe.interrupt()
    await fe.wait_done(r0, r1, timeout=10)
    assert fe.k.control_ack.get("interrupt") == tok
    assert fe.req(r0)["state"] == "error"
    assert fe.cell(c0)["outputs"][-1]["ename"] == "KeyboardInterrupt"
    assert fe.req(r1)["state"] == "cancelled"


async def test_restart(fe):
    c0 = fe.add_cell("x = 1\nx")
    [r] = fe.execute(c0)
    await fe.wait_done(r)
    c1 = fe.add_cell("import time\ntime.sleep(30)")
    c2 = fe.add_cell("print('queued')")
    r1, r2 = fe.execute(c1, c2)
    await wait_until(lambda: fe.req(r1).get("state") == "running")
    fe.restart()
    await fe.wait_done(r1, r2, timeout=30)
    assert fe.req(r1)["state"] == "cancelled" and fe.req(r2)["state"] == "cancelled"
    await wait_until(lambda: fe.k.status.get("execution_state") == "idle", 30)
    assert fe.k.status["generation"] == 1
    c3 = fe.add_cell("'x' in globals()")
    [r3] = fe.execute(c3)
    await fe.wait_done(r3)
    assert fe.cell(c3)["execution_count"] == 1
    assert fe.cell(c3)["outputs"][0]["data"]["text/plain"] == "False"


async def test_stale_generation_request_is_cancelled(fe):
    # A client that has not yet seen a restart writes gen=old
    cid = fe.add_cell("1")
    with fe.kdoc.transaction():
        fe.k.queue["stale"] = {"cell_id": cid, "seq": 99, "client": 1, "gen": -1, "state": "pending"}
    await fe.wait_done("stale")
    assert fe.req("stale")["state"] == "cancelled"


async def test_input(fe):
    cid = fe.add_cell("name = input('name? ')\nprint('hi', name)")
    [r] = fe.execute(cid)
    rid = await wait_until(lambda: next(iter(fe.k.stdin.keys()), None))
    assert fe.k.stdin[rid]["prompt"] == "name? "
    assert fe.k.stdin[rid]["cell_id"] == cid
    fe.answer_input(rid, "Ada")
    await fe.wait_done(r)
    assert fe.outputs_text(cid) == "name? Ada\nhi Ada\n"
    assert len(fe.k.stdin) == 0


async def test_display_update_across_cells_and_clear_wait(fe):
    c0 = fe.add_cell("h = display('first', display_id=True)")
    c1 = fe.add_cell("h.update('second')")
    c2 = fe.add_cell(
        "from IPython.display import clear_output\n"
        "for i in range(5):\n    clear_output(wait=True)\n    print(i)\n"
    )
    rids = fe.execute(c0, c1, c2)
    await fe.wait_done(*rids)
    assert fe.cell(c0)["outputs"][0]["data"]["text/plain"] == "'second'"
    assert fe.cell(c1)["outputs"] == []
    assert fe.outputs_text(c2) == "4\n"


async def test_completion_roundtrip(fe):
    fe.add_cell("")
    times = []
    for _ in range(20):
        rep, ms = await fe.complete("import o")
        times.append(ms)
        assert "os" in rep["content"]["matches"]
    med = statistics.median(times)
    print(f"\ncompletion round trip over Y: median {med:.1f} ms, min {min(times):.1f}, max {max(times):.1f}")
    assert len(fe.k.requests) == 0 and len(fe.k.replies) == 0


PROGRESS = (
    "import sys, time\n"
    "for i in range(1, 1001):\n"
    "    sys.stdout.write(f'\\r{i:4d}/1000 [' + '#' * (i // 50) + ' ' * (20 - i // 50) + ']')\n"
    "    sys.stdout.flush()\n"
    "    time.sleep(0.002)\n"
    "print()\n"
)


@pytest.mark.parametrize(
    "proxy",
    [dict(flush_interval=0.0), dict(flush_interval=0.05), dict(flush_interval=0.1),
     dict(flush_interval=0.0, terminal=False), dict(flush_interval=0.1, terminal=False)],
    indirect=True, ids=["nothrottle", "50ms", "100ms", "raw-nothrottle", "raw-100ms"])
async def test_progress_volume(fe, proxy):
    cid = fe.add_cell(PROGRESS)
    await asyncio.sleep(0.2)
    before_updates, before_bytes = proxy.stats["y_updates"], proxy.stats["y_bytes"]
    size_before = len(proxy.nbdoc.get_update())
    t0 = time.perf_counter()
    [r] = fe.execute(cid)
    await fe.wait_done(r, timeout=60)
    dt = time.perf_counter() - t0
    await asyncio.sleep(0.2)
    text = fe.outputs_text(cid)
    if proxy.terminal:
        assert text == "1000/1000 [" + "#" * 20 + "]\n"
    else:
        assert text.endswith("1000/1000 [" + "#" * 20 + "]\n") and text.count("\r") == 1000
    upd = proxy.stats["y_updates"] - before_updates
    byt = proxy.stats["y_bytes"] - before_bytes
    size_after = len(proxy.nbdoc.get_update())
    fresh = Doc()
    from jupyter_ydoc import YNotebook
    YNotebook(fresh).set(proxy.nb.get())
    compact = len(fresh.get_update())
    print(f"\n[flush={proxy.flush_interval} terminal={proxy.terminal}] exec {dt:.2f}s, Y updates={upd}, update bytes={byt}, "
          f"doc size {size_before}->{size_after} B (+{size_after - size_before}), "
          f"re-encoded from JSON: {compact} B")


async def test_execution_latency(fe):
    cid = fe.add_cell("1")
    times = []
    for _ in range(15):
        t0 = time.perf_counter()
        [r] = fe.execute(cid)
        await fe.wait_done(r)
        times.append((time.perf_counter() - t0) * 1000)
    print(f"\nintent -> result visible to client: median {statistics.median(times):.1f} ms, "
          f"min {min(times):.1f}, max {max(times):.1f}")


async def test_move_while_streaming_fast(fe):
    cid = fe.add_cell("import time\nfor i in range(200):\n    print(i, flush=True)\n    time.sleep(0.005)")
    fe.add_cell("pass")
    fe.add_cell("pass")
    [r] = fe.execute(cid)
    await wait_until(lambda: fe.req(r).get("state") == "running")
    for k in range(10):
        await asyncio.sleep(0.07)
        idx = fe.cell_ids().index(cid)
        fe.move_cell(idx, (idx + 1) % 3)
    await fe.wait_done(r)
    await asyncio.sleep(0.3)
    assert fe.outputs_text(cid) == "".join(f"{i}\n" for i in range(200))
    assert fe.cell_ids().count(cid) == 1


async def test_clear_then_move_after_grace_keeps_cleared(fe):
    cid = fe.add_cell("print('x')")
    [r] = fe.execute(cid)
    await fe.wait_done(r)
    # user clears outputs (in place), then moves the cell
    for c in fe.nb.ycells:
        if c["id"] == cid:
            c["outputs"].clear()
    fe.add_cell("pass")
    await asyncio.sleep(3.5)  # > reconcile_grace
    fe.move_cell(0, 1)
    await asyncio.sleep(0.3)
    assert fe.cell(cid)["outputs"] == []


async def test_interrupt_while_waiting_for_input(fe):
    cid = fe.add_cell("input('? ')")
    [r] = fe.execute(cid)
    await wait_until(lambda: len(fe.k.stdin) == 1)
    fe.interrupt()
    await fe.wait_done(r)
    assert fe.req(r)["state"] == "error"
    await wait_until(lambda: len(fe.k.stdin) == 0)


async def test_proxy_restart_recovers_orphans(server):
    async with KernelProxy(server.port, NB, KR) as p1:
        async with Frontend(server.port, NB, KR) as f:
            await wait_until(lambda: f.k.status.get("execution_state") == "idle")
            cid = f.add_cell("import time\ntime.sleep(30)")
            c2 = f.add_cell("1")
            r1, r2 = f.execute(cid, c2)
            await wait_until(lambda: f.req(r1).get("state") == "running")
    # proxy (and its kernel) died with an execution in flight
    async with KernelProxy(server.port, NB, KR) as p2:
        async with Frontend(server.port, NB, KR) as f:
            await wait_until(lambda: f.k.status.get("execution_state") == "idle")
            assert f.req(r1)["state"] == "cancelled"
            assert f.req(r2)["state"] == "cancelled"
            assert f.cell(cid)["execution_state"] == "idle"


@pytest.mark.parametrize("mininterval", [0.1, 0.0])
@pytest.mark.parametrize("proxy", [dict(flush_interval=0.0), dict(flush_interval=0.1)], indirect=True,
                         ids=["nothrottle", "100ms"])
async def test_real_tqdm_volume(fe, proxy, mininterval):
    pytest.importorskip("tqdm")
    cid = fe.add_cell(
        "import time\nfrom tqdm import tqdm\n"
        f"for _ in tqdm(range(1000), mininterval={mininterval}, ascii=True):\n    time.sleep(0.002)\n")
    await asyncio.sleep(0.2)
    u0 = proxy.stats["y_updates"]
    [r] = fe.execute(cid)
    await fe.wait_done(r, timeout=60)
    await asyncio.sleep(0.2)
    out = fe.cell(cid)["outputs"]
    assert out[-1]["name"] == "stderr" and "1000/1000" in out[-1]["text"] and "\r" not in out[-1]["text"]
    print(f"\n[tqdm mininterval={mininterval} proxy flush={proxy.flush_interval}] "
          f"Y updates={proxy.stats['y_updates'] - u0}, final stderr text {len(out[-1]['text'])} chars")
