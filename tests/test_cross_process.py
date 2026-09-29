"""跨进程恢复测试：真实子进程在外部效果已生效、确认未落库时被杀死。

与单进程测试不同，这里用 subprocess 启动一个真正的驱动进程，
在它被 SIGKILL 后由新进程（新的 Engine 实例）重开同一运行，
验证：

* 步骤停在 started，适配器台账已有回执；
* 恢复使用同一 op_id，不产生第二次外部效果；
* 运行最终 completed，终态后再 resume 不重复。
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


WORKER = r"""
import asyncio, os, sys, sqlite3
sys.path.insert(0, %(root)r)
from durable_saga import Engine, Store, Registry, Step, AmbiguousOutcome, Committed, NotExecuted, Unknown

class Ledger:
    def __init__(self, path):
        self.path = path
        c = sqlite3.connect(path)
        c.execute("PRAGMA journal_mode=DELETE")
        c.execute("create table if not exists effects(op_id primary key, payload)")
        c.commit(); c.close()
    def _c(self):
        c = sqlite3.connect(self.path)
        c.execute("PRAGMA journal_mode=DELETE")
        return c
    async def op(self, ctx, p, r):
        c = self._c()
        row = c.execute("select payload from effects where op_id=?", (ctx.op_id,)).fetchone()
        if row:
            import json; return json.loads(row[0])
        import json
        payload = {"v": p["v"], "op_id": ctx.op_id}
        c.execute("insert into effects(op_id, payload) values (?,?)",
                  (ctx.op_id, json.dumps(payload)))
        c.commit(); c.close()
        if p.get("kill"):
            os._exit(42)  # 外部效果已提交，执行库尚未记录成功
        return payload
    async def resolve(self, ctx, p, r):
        import json, sqlite3
        c = sqlite3.connect(self.path)
        row = c.execute("select payload from effects where op_id=?", (ctx.op_id,)).fetchone()
        c.close()
        return Committed(json.loads(row[0])) if row else NotExecuted()

async def main():
    run_id, runs_db, led_db = sys.argv[1], sys.argv[2], sys.argv[3]
    led = Ledger(led_db)
    reg = Registry()
    reg.handler("op", led.op)
    reg.handler("resolve", led.resolve)
    reg.define("f", 1, [
        Step("a", "op", params={"v": "A", "kill": True}, resolver_ref="resolve"),
        Step("b", "op", params={"v": "B"}),
    ])
    eng = Engine(Store(runs_db), reg, lease_ttl_seconds=1.0)
    await eng.start("f", run_id=run_id, drive=False)
    await eng.resume(run_id)

asyncio.run(main())
"""


class TestCrossProcessCrash(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.runs_db = os.path.join(self.tmp, "runs.db")
        self.led_db = os.path.join(self.tmp, "ledger.db")
        self.run_id = "xproc-run"

    def _engine(self, ttl=1.0):
        # 延迟导入，worker 里也用同一套
        sys.path.insert(0, ROOT)
        from durable_saga import Engine, Registry, Step, Store

        led = _WorkerLedger(self.led_db)
        reg = Registry()
        reg.handler("op", led.op)
        reg.handler("resolve", led.resolve)
        reg.define("f", 1, [
            Step("a", "op", params={"v": "A"}, resolver_ref="resolve"),
            Step("b", "op", params={"v": "B"}),
        ])
        return Engine(Store(self.runs_db), reg, lease_ttl_seconds=ttl)

    def _create_run(self):
        pass  # 运行由 worker 进程自己创建

    def test_killed_process_recovered_by_new_process(self):
        import asyncio

        self._create_run()

        worker_path = os.path.join(self.tmp, "worker.py")
        with open(worker_path, "w") as f:
            f.write(WORKER % {"root": ROOT})

        proc = subprocess.run(
            [sys.executable, worker_path, self.run_id,
             self.runs_db, self.led_db],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 42, proc.stderr)

        # 新进程视角：步骤 started，适配器已有效果
        eng = self._engine()
        view = asyncio.run(eng.open(self.run_id))
        self.assertEqual(view.step("a").state, "started")
        effects = self._ledger_rows()
        self.assertEqual(len(effects), 1)
        self.assertEqual(effects[0][0], f"{self.run_id}:a:1")

        # 等租约过期，新引擎恢复
        import time
        time.sleep(1.1)
        view2 = asyncio.run(eng.resume(self.run_id))
        self.assertEqual(view2.state, "completed")
        self.assertEqual(
            view2.step("a").result["op_id"], f"{self.run_id}:a:1"
        )
        # 适配器只有两次效果：a（被杀前）+ b；a 没有第二次
        effects2 = self._ledger_rows()
        self.assertEqual([r[0] for r in effects2],
                         [f"{self.run_id}:a:1", f"{self.run_id}:b:1"])

        # 终态再 resume 不产生新效果
        asyncio.run(eng.resume(self.run_id))
        self.assertEqual(len(self._ledger_rows()), 2)

    def _ledger_rows(self):
        conn = sqlite3.connect(self.led_db)
        rows = list(conn.execute(
            "select op_id from effects order by rowid"))
        conn.close()
        return rows


class _WorkerLedger:
    """测试主进程里用的同语义适配器。"""

    def __init__(self, path):
        self.path = path
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute(
            "create table if not exists effects"
            "(op_id primary key, payload)"
        )
        conn.commit()
        conn.close()

    async def op(self, ctx, p, r):
        import json
        conn = sqlite3.connect(self.path)
        row = conn.execute(
            "select payload from effects where op_id=?", (ctx.op_id,)
        ).fetchone()
        if row:
            conn.close()
            return json.loads(row[0])
        payload = {"v": p["v"], "op_id": ctx.op_id}
        conn.execute(
            "insert into effects(op_id, payload) values (?,?)",
            (ctx.op_id, json.dumps(payload)),
        )
        conn.commit()
        conn.close()
        return payload

    async def resolve(self, ctx, p, r):
        import json
        from durable_saga import Committed, NotExecuted

        conn = sqlite3.connect(self.path)
        row = conn.execute(
            "select payload from effects where op_id=?", (ctx.op_id,)
        ).fetchone()
        conn.close()
        return Committed(json.loads(row[0])) if row else NotExecuted()


if __name__ == "__main__":
    unittest.main(verbosity=2)
