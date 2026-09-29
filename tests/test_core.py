"""库核心状态机测试：用内存版 handler 覆盖崩溃/恢复/取消/版本化语义。"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from durable_calibration import (  # noqa: E402
    CalibrationLibrary,
    Flow,
    FlowStep,
    OperationRejected,
    RunLocked,
    compensate_when_field_equals,
)
from durable_calibration import models as M  # noqa: E402


def run(coro):
    return asyncio.run(coro)


def make_flow(handlers, *, with_comp=True, predicate=None, version=1):
    steps = [
        FlowStep("s1", "h1", params={"x": 1}),
        FlowStep("s2", "h2", params={"y": 2},
                 compensator="c2" if with_comp else None,
                 compensate_when=predicate),
    ]
    return Flow.define("f", steps, version=version)


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "runs.db")

        self.calls = []

        async def h1(ctx, params):
            self.calls.append(("h1", ctx.op_id, ctx.replay))
            return {"v": (params or {}).get("x", 0) + 1}

        async def h2(ctx, params):
            self.calls.append(("h2", ctx.op_id, ctx.replay))
            return {"v": params["y"] + 1}

        async def c2(ctx, params, result):
            self.calls.append(("c2", ctx.op_id, ctx.replay, result))
            return {"undone": True}

        self.h1, self.h2, self.c2 = h1, h2, c2
        self.lib = CalibrationLibrary(self.db_path)
        self.lib.register_handlers(h1=h1, h2=h2, c2=c2)

    def tearDown(self):
        self.lib.db.close()
        self.tmp.cleanup()

    def _start(self, flow):
        self.lib.register_flow(flow)
        return run(self.lib.start_run("f"))

    def test_happy_path_persists_independent_results(self):
        rid = self._start(make_flow(None))
        snap = run(self.lib.run(rid))
        self.assertEqual(snap.status, M.RUN_SUCCEEDED)
        self.assertEqual([s.execute_status for s in snap.steps],
                         [M.STEP_SUCCEEDED, M.STEP_SUCCEEDED])
        self.assertEqual(snap.result("s1"), {"v": 2})
        self.assertEqual(snap.result("s2"), {"v": 3})
        # 每个尝试独立留痕，终态后没有撤回。
        execs = [s.execute_attempt for s in snap.steps]
        self.assertTrue(all(a.status == M.ATTEMPT_SUCCEEDED for a in execs))
        self.assertTrue(all(s.compensate_status in (M.COMP_NOT_NEEDED,)
                            or s.compensate_status == M.COMP_PENDING
                            for s in snap.steps))
        kinds = {f.kind for f in snap.facts}
        self.assertIn("run_succeeded", kinds)
        self.assertNotIn("compensation_entered", kinds)

    def test_reopen_terminal_run_is_idempotent(self):
        rid = self._start(make_flow(None))
        run(self.lib.run(rid))
        # 模拟重启：新建库实例、重新登记，再 resume 终态运行。
        lib2 = CalibrationLibrary(self.db_path)
        lib2.register_handlers(h1=self.h1, h2=self.h2, c2=self.c2)
        lib2.register_flow(make_flow(None))
        snap = run(lib2.resume(rid))
        self.assertEqual(snap.status, M.RUN_SUCCEEDED)
        # 没有产生新的执行尝试。
        self.assertEqual(len([c for c in self.calls if c[0].startswith("h")]), 2)

    def test_validation_failure_compensates_in_reverse(self):
        async def h2_fail(ctx, params):
            return {"passed": False, "current": 9}

        async def c2(ctx, params, result):
            self.calls.append(("c2", ctx.op_id, result))
            return {"restored": 1}

        self.lib.register_handlers(h2=h2_fail, c2=c2)
        flow = Flow.define(
            "f",
            [FlowStep("s1", "h1"),
             FlowStep("s2", "h2", compensator="c2",
                      compensate_when=compensate_when_field_equals(
                          "passed", False))],
            version=2,
        )
        rid = self._start(flow)
        snap = run(self.lib.run(rid))
        self.assertEqual(snap.status, M.RUN_COMPENSATED)
        s2 = next(s for s in snap.steps if s.key == "s2")
        self.assertEqual(s2.execute_status, M.STEP_SUCCEEDED)
        self.assertEqual(s2.compensate_status, M.COMP_SUCCEEDED)
        # 撤回顺序：只有 s2 需要撤；补偿拿到的是确认结果。
        comp_calls = [c for c in self.calls if c[0] == "c2"]
        self.assertEqual(comp_calls[0][2], {"passed": False, "current": 9})

    def test_explicit_reject_triggers_compensation_and_allows_new_op(self):
        async def h2_reject(ctx, params):
            raise OperationRejected("bad params", receipt={"code": 41})

        async def c2(ctx, params, result):  # 不应被调用
            raise AssertionError("被拒绝的步骤确定无效果，不应撤回它")

        self.lib.register_handlers(h2=h2_reject, c2=c2)
        flow = Flow.define("f", [FlowStep("s1", "h1"),
                                 FlowStep("s2", "h2", compensator="c2")])
        rid = self._start(flow)
        snap = run(self.lib.run(rid))
        self.assertEqual(snap.status, M.RUN_COMPENSATED)
        s2 = snap.steps[1]
        self.assertEqual(s2.execute_status, M.STEP_REJECTED)
        self.assertEqual(s2.error["type"], "OperationRejected")
        self.assertEqual(s2.compensate_status, M.COMP_NOT_NEEDED)

    def test_inconclusive_execute_is_replayed_with_same_op_id(self):
        state = {"fail": True}

        async def h2_flaky(ctx, params):
            self.calls.append(("h2", ctx.op_id, ctx.replay))
            if state["fail"] and not ctx.replay:
                state["fail"] = False
                raise ConnectionError("timeout: ack lost")
            return {"v": 42}

        self.lib.register_handlers(h2=h2_flaky)
        rid = self._start(make_flow(None))
        # max_replays=0 模拟进程在结果不明时立即终止：不在同进程内重试。
        snap1 = run(self.lib.run(rid, max_replays=0))
        self.assertEqual(snap1.status, M.RUN_AWAITING_CONFIRMATION)
        self.assertEqual(snap1.steps[1].execute_status, M.STEP_INCONCLUSIVE)
        op1 = snap1.steps[1].execute_attempt.op_id

        # 适配器恢复后重开：必须使用同一 op_id 做再确认，不生成新操作。
        snap2 = run(self.lib.resume(rid))
        self.assertEqual(snap2.status, M.RUN_SUCCEEDED)
        att = snap2.steps[1].execute_attempt
        self.assertEqual(att.op_id, op1)
        self.assertEqual(att.status, M.ATTEMPT_SUCCEEDED)
        h2_calls = [c for c in self.calls if c[0] == "h2"]
        self.assertEqual(len(h2_calls), 2)
        self.assertEqual(h2_calls[0][1], h2_calls[1][1])  # 同一 op_id
        self.assertEqual((h2_calls[0][2], h2_calls[1][2]), (False, True))

    def test_persistent_inconclusive_is_bounded_not_busy_looped(self):
        async def h2_always_broken(ctx, params):
            self.calls.append(("h2", ctx.op_id, ctx.replay))
            raise ConnectionError("adapter down")

        self.lib.register_handlers(h2=h2_always_broken)
        rid = self._start(make_flow(None))
        snap = run(self.lib.run(rid, max_replays=2))
        # 适配器一直没恢复：本次驱动在 1 次初调 + 2 次重放后停在阻塞态，
        # 开放尝试（同一 op_id）保留，等待外部恢复后 resume。
        self.assertEqual(snap.status, M.RUN_AWAITING_CONFIRMATION)
        self.assertEqual(len([c for c in self.calls if c[0] == "h2"]), 3)
        op_ids = {c[1] for c in self.calls if c[0] == "h2"}
        self.assertEqual(len(op_ids), 1)  # 全程同一个 op_id
        self.assertEqual(snap.steps[1].execute_status, M.STEP_INCONCLUSIVE)
        self.assertEqual(snap.steps[1].execute_attempt.status,
                         M.ATTEMPT_INCONCLUSIVE)

    def test_inconclusive_compensation_blocks_and_resumes(self):
        state = {"fail": True}

        async def h2_fail(ctx, params):
            return {"passed": False}

        async def c2_flaky(ctx, params, result):
            self.calls.append(("c2", ctx.op_id, ctx.replay))
            if state["fail"] and not ctx.replay:
                state["fail"] = False
                raise ConnectionError("restore timeout")
            return {"restored": True}

        self.lib.register_handlers(h2=h2_fail, c2=c2_flaky)
        flow = Flow.define(
            "f",
            [FlowStep("s1", "h1"),
             FlowStep("s2", "h2", compensator="c2",
                      compensate_when=compensate_when_field_equals(
                          "passed", False))],
        )
        rid = self._start(flow)
        snap1 = run(self.lib.run(rid, max_replays=0))
        self.assertEqual(snap1.status, M.RUN_COMPENSATION_BLOCKED)
        self.assertIsNotNone(snap1.blocked_reason)
        self.assertEqual(snap1.blocked_reason["code"], "inconclusive_compensate")
        op1 = snap1.steps[1].compensate_attempt.op_id

        snap2 = run(self.lib.resume(rid))
        self.assertEqual(snap2.status, M.RUN_COMPENSATED)
        self.assertEqual(snap2.steps[1].compensate_attempt.op_id, op1)
        c_calls = [c for c in self.calls if c[0] == "c2"]
        self.assertEqual(len(c_calls), 2)
        self.assertEqual(c_calls[0][1], c_calls[1][1])

    def test_rejected_compensation_retries_with_new_op_id(self):
        attempts = {"n": 0}

        async def h2_fail(ctx, params):
            return {"passed": False}

        async def c2_reject_twice(ctx, params, result):
            attempts["n"] += 1
            self.calls.append(("c2", ctx.op_id, ctx.replay))
            if attempts["n"] < 3:
                raise OperationRejected("busy")
            return {"restored": True}

        self.lib.register_handlers(h2=h2_fail, c2=c2_reject_twice)
        flow = Flow.define(
            "f",
            [FlowStep("s1", "h1"),
             FlowStep("s2", "h2", compensator="c2",
                      compensate_when=compensate_when_field_equals(
                          "passed", False))],
            version=3,
        )
        rid = self._start(flow)
        snap1 = run(self.lib.run(rid))
        # 第一次拒绝后运行仍可继续：本次驱动循环内会再领到新补偿尝试。
        self.assertEqual(snap1.status, M.RUN_COMPENSATED)
        comp_attempts = self.lib.db._conn.execute(
            "SELECT op_id FROM run_attempts WHERE step_key='s2' AND kind='compensate'"
        ).fetchall()
        op_ids = [r["op_id"] for r in comp_attempts]
        self.assertEqual(len(op_ids), 3)
        self.assertEqual(len(set(op_ids)), 3)  # 明确拒绝 -> 新操作身份

    def test_cancel_request_drives_compensation(self):
        gate = asyncio.Event()

        async def h2_wait(ctx, params):
            self.calls.append("h2-enter")
            await gate.wait()
            return {"v": 1}

        self.lib.register_handlers(h2=h2_wait)
        rid = self._start(make_flow(None))

        async def scenario():
            task = asyncio.create_task(self.lib.run(rid))
            await asyncio.sleep(0.05)
            await self.lib.request_cancel(rid)
            # h2 尚未确认；取消必须等其结果明确后才撤回，不能与前向交织。
            gate.set()
            return await task

        snap = run(scenario())
        self.assertIn(snap.status,
                      (M.RUN_COMPENSATED, M.RUN_AWAITING_CONFIRMATION))

    def test_cancel_before_effects_is_cancelled(self):
        async def h1_sleep(ctx, params):
            await asyncio.sleep(0.2)
            return {"v": 1}

        self.lib.register_handlers(h1=h1_sleep)
        flow = Flow.define("f", [FlowStep("s1", "h1")])
        rid = self._start(flow)

        async def scenario():
            # s1 的 started 尝试已落库但外部结果未定；这里直接模拟进程被杀：
            # 不等待 task，直接看取消+恢复的语义。
            await self.lib.request_cancel(rid)
            # 用一个立即返回的 h1 实现重放（等价于适配器确认操作已完成）。
            async def h1_done(ctx, params):
                self.calls.append(("h1", ctx.op_id, ctx.replay))
                return {"v": 1}

            self.lib.register_handlers(h1=h1_done)
            return await self.lib.resume(rid)

        snap = run(scenario())
        # s1 确认成功但其本身没有恢复函数；取消时没有需要撤回的外部操作被记录，
        # 终态为 cancelled。若 s1 带 compensator，则会是 compensated。
        self.assertEqual(snap.status, M.RUN_CANCELLED)

    def test_missing_handler_blocks_with_explicit_reason(self):
        rid = self._start(make_flow(None))
        # 新库实例只登记 h1（模拟服务升级后旧步骤函数被移除）。
        lib2 = CalibrationLibrary(self.db_path)
        lib2.register_handlers(h1=self.h1)
        snap = run(lib2.resume(rid))
        self.assertEqual(snap.status, M.RUN_AWAITING_CONFIRMATION)
        self.assertEqual(snap.blocked_reason["code"], "handler_unavailable")
        self.assertEqual(snap.blocked_reason["handler"], "h2")
        self.assertEqual(snap.blocked_reason["step"], "s2")

    def test_old_run_keeps_old_definition(self):
        flow_v1 = Flow.define(
            "svc", [FlowStep("a", "h1")], version=1, title="old")
        self.lib.register_flow(flow_v1)
        rid = run(self.lib.start_run("svc", flow_version=1))
        definition = self.lib.load_flow_definition(rid)
        self.assertEqual([s["key"] for s in definition["steps"]], ["a"])
        self.assertEqual(definition["version"], 1)

    def test_service_upgrade_old_run_uses_old_handlers_or_blocks(self):
        # v1 流程用 h1/h2；登记并登记一条运行（先不驱动）。
        flow_v1 = Flow.define(
            "svc",
            [FlowStep("a", "h1"), FlowStep("b", "h2", compensator="c2")],
            version=1, title="v1")
        self.lib.register_flow(flow_v1)
        rid = run(self.lib.start_run("svc", flow_version=1))

        # 服务升级：h2 被移除，新增 h3，v2 流程含义不同。
        async def h3(ctx, params):
            raise AssertionError("旧运行绝不能调用新版本的处理函数")

        lib_upgraded = CalibrationLibrary(self.db_path)
        lib_upgraded.register_handlers(h1=self.h1, c2=self.c2, h3=h3)
        flow_v2 = Flow.define(
            "svc", [FlowStep("a", "h1"), FlowStep("c", "h3")], version=2,
            title="v2")
        lib_upgraded.register_flow(flow_v2)

        # 旧运行仍按 v1 解释：缺 h2 -> 明确阻塞，不会换成 h3 接着跑。
        snap = run(lib_upgraded.resume(rid))
        self.assertEqual(snap.status, M.RUN_AWAITING_CONFIRMATION)
        self.assertEqual(snap.blocked_reason["code"], "handler_unavailable")
        self.assertEqual(snap.blocked_reason["handler"], "h2")
        # 固化的定义仍是 v1（含步骤 b / 恢复函数 c2）。
        old_def = lib_upgraded.load_flow_definition(rid)
        self.assertEqual(old_def["version"], 1)
        self.assertEqual([s["key"] for s in old_def["steps"]], ["a", "b"])
        self.assertEqual(old_def["steps"][1]["compensator"], "c2")

    def test_run_input_placeholder_resolution(self):
        flow = Flow.define(
            "p",
            [FlowStep("s1", "h1",
                      params={"got": {"$from_run_input": "cfg.value"}})],
        )
        self.lib.register_flow(flow)
        rid = run(self.lib.start_run("p", run_input={"cfg": {"value": 7}}))
        snap = run(self.lib.run(rid))
        self.assertEqual(snap.result("s1"), {"v": 1})  # h1 ignores params
        stored = self.lib.load_flow_definition(rid)
        self.assertEqual(stored["steps"][0]["params"]["got"], 7)

    def test_compensator_only_sees_confirmed_result(self):
        seen = {}

        async def c2(ctx, params, result):
            seen["result"] = result
            seen["results"] = dict(ctx.results)
            return {"ok": True}

        self.lib.register_handlers(c2=c2)
        async def h2(ctx, params):
            return {"passed": False}
        self.lib.register_handlers(h2=h2)
        flow = Flow.define(
            "f",
            [FlowStep("s1", "h1"),
             FlowStep("s2", "h2", compensator="c2",
                      compensate_when=compensate_when_field_equals(
                          "passed", False))],
            version=4,
        )
        rid = self._start(flow)
        snap = run(self.lib.run(rid))
        self.assertEqual(snap.status, M.RUN_COMPENSATED)
        self.assertEqual(seen["result"]["passed"], False)
        self.assertEqual(seen["results"]["s1"]["v"], 1)


class LeaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "runs.db")

        async def h(ctx, params):
            return {"ok": True}

        self.lib = CalibrationLibrary(self.db_path)
        self.lib.register_handlers(h=h)
        self.lib.register_flow(Flow.define("g", [FlowStep("a", "h")]))
        self.rid = run(self.lib.start_run("g"))

    def tearDown(self):
        self.lib.db.close()
        self.tmp.cleanup()

    def test_second_driver_blocked_until_lease_expires(self):
        import datetime as dt

        fixed = [dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)]

        def clock():
            return fixed[0]

        lib_a = CalibrationLibrary(self.db_path)
        lib_a.db._now_fn = clock
        lib_a.register_handlers(h=_slow_handler(fixed))

        epoch = lib_a.db.acquire_lease(self.rid, "ownerA", ttl_seconds=10)
        self.assertIsNotNone(epoch)

        lib_b = CalibrationLibrary(self.db_path)
        lib_b.db._now_fn = clock
        # 未过期：拒绝接管。
        self.assertIsNone(
            lib_b.db.acquire_lease(self.rid, "ownerB", ttl_seconds=10,
                                   force=True))
        # 时钟越过 TTL：可以接管，epoch 递增 fencing 掉 A。
        fixed[0] += dt.timedelta(seconds=11)
        epoch_b = lib_b.db.acquire_lease(self.rid, "ownerB",
                                         ttl_seconds=10, force=True)
        self.assertEqual(epoch_b, epoch + 1)
        # A 的心跳与提交都会被 fencing 拒绝。
        from durable_calibration.errors import RunLockLost
        with self.assertRaises(RunLockLost):
            lib_a.db.heartbeat(self.rid, "ownerA", epoch, 10)
        lib_a.db.close()
        lib_b.db.close()


def _slow_handler(clock_state):
    async def h(ctx, params):
        return {"ok": True}
    return h


    def test_concurrent_drivers_only_one_makes_progress(self):
        # 两个独立的库实例（模拟两个采集进程）指向同一份执行记录。
        lib_b = CalibrationLibrary(self.db_path)
        lib_b.register_handlers(h1=self.h1, h2=self.h2, c2=self.c2)

        rid = self._start(make_flow(None))

        async def scenario():
            # 同时驱动：其中一个拿到租约，另一个必须收到 RunLocked。
            results = await asyncio.gather(
                self.lib.run(rid),
                lib_b.resume(rid),
                return_exceptions=True,
            )
            statuses = []
            for r in results:
                if isinstance(r, Exception):
                    statuses.append(type(r).__name__)
                else:
                    statuses.append(r.status)
            return results, statuses

        results, statuses = run(scenario())
        self.assertIn("RunLocked", statuses)
        self.assertIn(M.RUN_SUCCEEDED, statuses)
        # 只有一套前向进展：每个执行步骤恰好一次成功尝试。
        snap = run(self.lib.get_run(rid))
        for s in snap.steps:
            self.assertEqual(s.execute_attempt.status, M.ATTEMPT_SUCCEEDED)
        # h1/h2 各自只被调用一次（没有双写）。
        self.assertEqual(len([c for c in self.calls if c[0] == "h1"]), 1)
        self.assertEqual(len([c for c in self.calls if c[0] == "h2"]), 1)
        lib_b.db.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
