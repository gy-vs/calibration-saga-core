"""真实进程级集成测试：用 subprocess 启动 worker，在写入后 SIGKILL。

这些测试验证的是单元测试无法覆盖的事实：
- 控制器独立存储已经应用了新参数，但执行库的记录只到 ``step_started``；
- 全新进程通过同一 op_id 查到最初回执，不重复写入；
- 验证失败时新进程（或同进程）能完成恢复，仪器回到原参数；
- 另一个进程在租约未过期时无法接管。
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "examples" / "worker.py"
ENV = dict(os.environ, PYTHONPATH=f"{ROOT / 'src'}:{ROOT}",
           DCAL_TTL="2")
ORIGINAL = {"gain": 1.0, "offset": 0.0}
NEW = {"gain": 2.5, "offset": 0.125}


def _run_worker(base, cmd, run_id=None, timeout=25, env=None):
    # worker 参数顺序：<cmd> <base> <run_id>
    args = [cmd, str(base)]
    if run_id is not None:
        args.append(run_id)
    proc = subprocess.run(
        [sys.executable, str(WORKER), *args],
        capture_output=True, text=True, timeout=timeout,
        env=env or ENV,
    )
    return proc


def _scenario(base, **flags):
    Path(base, "scenario.json").write_text(json.dumps(flags))


def _inspect(base, run_id):
    proc = _run_worker(base, "inspect", run_id)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def _controller_params(base):
    db = Path(base) / "controller" / "controller_state.db"
    if not db.exists():
        # 仪器从未被触碰时控制器还没有存储文件：即出厂原参数。
        return dict(ORIGINAL)
    conn = sqlite3.connect(db)
    try:
        payload = conn.execute(
            "SELECT payload FROM params WHERE id=1").fetchone()[0]
        return json.loads(payload)
    finally:
        conn.close()


def _controller_op_count(base, kind):
    db = Path(base) / "controller" / "controller_state.db"
    if not db.exists():
        return 0
    conn = sqlite3.connect(db)
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM operations WHERE kind=?", (kind,)
        ).fetchone()[0]
    finally:
        conn.close()


class KillRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dcal-it-")

    def test_happy_path_sets_final_params_and_is_stable_across_restart(self):
        rid = "it-happy"
        _scenario(self.tmp)
        proc = _run_worker(self.tmp, "start", rid)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        rpt = _inspect(self.tmp, rid)
        self.assertEqual(rpt["status"], "succeeded")
        self.assertEqual(rpt["instrument_params_now"], NEW)

        # 重启后再 resume：终态幂等，不产生任何新操作。
        proc = _run_worker(self.tmp, "resume", rid)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["status"], "succeeded")
        self.assertEqual(_controller_op_count(self.tmp, "write_parameters"), 1)
        self.assertEqual(_controller_params(self.tmp), NEW)

    def test_kill_after_write_then_resume_uses_original_op_id(self):
        rid = "it-kill"
        _scenario(self.tmp, kill_after_write=True,
                  kill_barrier=str(Path(self.tmp) / "barrier.json"))
        proc = _run_worker(self.tmp, "start", rid)
        # SIGKILL -> 负数信号或 137。
        self.assertIn(proc.returncode, (-9, 137))
        self.assertTrue(Path(self.tmp, "barrier.json").exists())

        # 仪器实际已是新参数，但库记录只表明 write 曾经开始。
        self.assertEqual(_controller_params(self.tmp), NEW)
        rpt = _inspect(self.tmp, rid)
        self.assertEqual(rpt["status"], "running")
        write_step = next(s for s in rpt["steps"]
                          if s["key"] == "write_parameters")
        self.assertEqual(write_step["execute_status"], "started")
        killed_op = write_step["execute_op_id"]

        # 模拟控制器/主机恢复：去掉 kill，等租约过期后用新进程 resume。
        _scenario(self.tmp)
        time.sleep(2.2)
        proc = _run_worker(self.tmp, "resume", rid)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["status"], "succeeded")

        rpt = _inspect(self.tmp, rid)
        write_step = next(s for s in rpt["steps"]
                          if s["key"] == "write_parameters")
        self.assertEqual(write_step["execute_status"], "succeeded")
        # 同一操作身份：没有重新生成一次写入。
        self.assertEqual(write_step["execute_op_id"], killed_op)
        self.assertEqual(_controller_op_count(self.tmp, "write_parameters"), 1)
        # 事实链明确记录了一次再确认。
        self.assertIn("attempt_replayed",
                      [f["kind"] for f in rpt["facts"]])
        self.assertEqual(_controller_params(self.tmp), NEW)

    def test_validation_failure_compensates_to_original_params(self):
        rid = "it-valfail"
        _scenario(self.tmp, force_validation_fail=True)
        proc = _run_worker(self.tmp, "start", rid)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        rpt = _inspect(self.tmp, rid)
        self.assertEqual(rpt["status"], "compensated")
        self.assertEqual(rpt["instrument_params_now"], ORIGINAL)
        write_step = next(s for s in rpt["steps"]
                          if s["key"] == "write_parameters")
        self.assertEqual(write_step["compensate_status"], "succeeded")
        self.assertIn("run_compensated", [f["kind"] for f in rpt["facts"]])
        # 正常完成的恢复结果在重启后不会被再次撤回或重写。
        proc = _run_worker(self.tmp, "resume", rid)
        self.assertEqual(json.loads(proc.stdout)["status"], "compensated")
        self.assertEqual(_controller_op_count(self.tmp, "restore_parameters"), 1)

    def test_compensation_rejection_retried_with_new_op_id_then_succeeds(self):
        rid = "it-restreject"
        _scenario(self.tmp, force_validation_fail=True, restore_reject_times=1)
        proc = _run_worker(self.tmp, "start", rid)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        rpt = _inspect(self.tmp, rid)
        self.assertEqual(rpt["status"], "compensated")
        write_step = next(s for s in rpt["steps"]
                          if s["key"] == "write_parameters")
        # 第一次恢复明确拒绝（compensate:1），第二次用新身份成功（compensate:2）。
        self.assertTrue(write_step["compensate_op_id"].endswith("compensate:2"))
        self.assertEqual(_controller_op_count(self.tmp, "restore_parameters"), 1)
        self.assertEqual(_controller_params(self.tmp), ORIGINAL)

    def test_second_process_cannot_take_over_until_lease_expires(self):
        rid = "it-lease"
        _scenario(self.tmp)

        # 用一个会停在长步骤上的 in-process 方式直接占住租约较难用子进程
        # 精确控制；这里改为：start 已快速结束，我们手工插入一把未过期租约，
        # 再让第二个进程 resume，应当被拒（RunLocked -> 非零退出）。
        runs_db = Path(self.tmp) / "runs.db"
        # 先正常登记并完成一次运行。
        proc = _run_worker(self.tmp, "start", rid)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        # 把租约伪造成一把由"别的活进程"持有且未过期的锁。
        conn = sqlite3.connect(runs_db)
        import datetime as dt
        future = (dt.datetime.now(dt.timezone.utc)
                  + dt.timedelta(seconds=60)).isoformat()
        conn.execute(
            "UPDATE flow_runs SET lock_owner='someone-else', lock_epoch=5,"
            " locked_until=? WHERE run_id=?", (future, rid))
        conn.commit()
        conn.close()

        proc = _run_worker(self.tmp, "resume", rid)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("RunLocked", proc.stderr)


    def test_cancel_before_effects_is_cancelled_without_touching_instrument(self):
        # 用一个尚未驱动过的运行：先 start 不行（会直接跑完）。直接在库内登记
        # 一条 pending 运行，再用 worker 的 cancel 命令（请求取消 + resume）。
        import asyncio
        sys.path.insert(0, str(ROOT / "src"))
        from durable_calibration import CalibrationLibrary  # noqa: E402
        from examples.calibration_flow import (  # noqa: E402
            build_flow, build_library, run_input)

        rid = "it-cancel"
        runs_db = str(Path(self.tmp) / "runs.db")

        async def register():
            lib = build_library(runs_db)
            lib.register_flow(build_flow(new_params=NEW))
            await lib.start_run(
                "instrument-calibration", flow_version=1,
                run_input=run_input(str(Path(self.tmp) / "controller"),
                                   NEW, str(Path(self.tmp) / "scenario.json")),
                run_id=rid,
            )
            await lib.close()

        asyncio.run(register())
        _scenario(self.tmp)

        proc = _run_worker(self.tmp, "cancel", rid)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["status"], "cancelled")
        # 仪器从未被触碰。
        self.assertEqual(_controller_op_count(self.tmp, "write_parameters"), 0)
        self.assertEqual(_controller_params(self.tmp), ORIGINAL)


if __name__ == "__main__":
    unittest.main(verbosity=2)
