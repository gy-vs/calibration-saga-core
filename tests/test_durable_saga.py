"""durable_saga 的标准库 unittest 测试（无 pytest 依赖）。

覆盖：
* 正常完成 / 验证失败回滚 / 步骤结果在步骤间传递；
* started 崩溃后用同一 op_id 恢复（幂等适配器约定）；
* resolver 返回 Committed/NotExecuted/Unknown 三种结论；
* 恢复动作失败后继续本次运行；
* 取消语义（开始前 / 生效后）；
* 租约并发与 fencing epoch；
* 重启后 recover_all；
* 流程版本快照与缺失处理函数的 incompatible；
* 终态幂等：completed 不会因重启再次写入或撤回。
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from durable_saga import (  # noqa: E402
    AmbiguousOutcome,
    Committed,
    Engine,
    NotExecuted,
    Registry,
    RunBusyError,
    IncompatibleRunError,
    Step,
    StepContext,
    StepFailed,
    Store,
    Unknown,
)
from durable_saga.store import LeaseLostError  # noqa: E402


def run(coro):
    return asyncio.run(coro)


class IdempotentAdapter:
    """模拟仪器：内存台账，op_id 幂等；可注入故障与崩溃点。"""

    def __init__(self):
        self.effects: dict[str, dict] = {}  # op_id -> result
        self.commits: list[str] = []
        self.offline = False
        self.fail_unknown_once = False
        self.crash_after_commit = False

    async def op(self, ctx: StepContext, params, results):
        if ctx.op_id in self.effects:
            return self.effects[ctx.op_id]
        if self.offline:
            return AmbiguousOutcome(reason="adapter offline")
        result = {"value": params.get("v"), "op_id": ctx.op_id}
        self.effects[ctx.op_id] = result
        self.commits.append(ctx.op_id)
        return result

    async def resolve(self, ctx: StepContext, params, results):
        if ctx.op_id in self.effects:
            return Committed(result=self.effects[ctx.op_id])
        if self.offline:
            return Unknown(reason="still offline")
        return NotExecuted()

    async def comp(self, ctx: StepContext, comp_params, step_result):
        if ctx.op_id in self.effects:
            return self.effects[ctx.op_id]
        if self.offline:
            return AmbiguousOutcome(reason="comp offline")
        result = {"undone": step_result["value"], "op_id": ctx.op_id}
        self.effects[ctx.op_id] = result
        self.commits.append(ctx.op_id)
        return result


def make_registry(adapter: IdempotentAdapter) -> Registry:
    reg = Registry()
    reg.handler("op", adapter.op)
    reg.handler("resolve", adapter.resolve)
    reg.handler("comp", adapter.comp)

    async def fail_verify(ctx, p, r):
        raise StepFailed("bad", {"detail": 1})

    async def flaky(ctx, p, r):
        # 第一次调用抛不明异常（进程活着但响应丢失），第二次靠幂等成功。
        if not getattr(flaky, "_called", False):
            flaky._called = True
            raise ConnectionError("lost response")
        return await adapter.op(ctx, p, r)

    reg.handler("fail_verify", fail_verify)
    reg.handler("flaky", flaky)

    reg.define("ok", 1, [
        Step("a", "op", params={"v": "A"}),
        Step("b", "op", params={"v": "B"}),
    ])
    reg.define("withcomp", 1, [
        Step("a", "op", params={"v": "A"}, compensation_ref="comp"),
        Step("b", "op", params={"v": "B"}, compensation_ref="comp"),
        Step("v", "fail_verify"),
    ])
    reg.define("resolved", 1, [
        Step("a", "op", params={"v": "A"},
             compensation_ref="comp", resolver_ref="resolve"),
        Step("b", "op", params={"v": "B"},
             compensation_ref="comp", resolver_ref="resolve"),
        Step("v", "fail_verify"),
    ])
    reg.define("flakyflow", 1, [
        Step("a", "flaky", params={"v": "A"}, resolver_ref="resolve"),
    ])
    return reg


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, "runs.db")
        self.adapter = IdempotentAdapter()
        self.reg = make_registry(self.adapter)

    def engine(self, ttl: float = 5.0, registry=None) -> Engine:
        return Engine(Store(self.db_path), registry or self.reg,
                      lease_ttl_seconds=ttl)


class TestHappyPath(BaseCase):
    def test_complete_and_results_flow(self):
        eng = self.engine()
        h = run(eng.start("ok"))
        v = run(eng.open(h.run_id))
        self.assertEqual(v.state, "completed")
        self.assertEqual(v.result, {"a": {"value": "A",
                                          "op_id": f"{h.run_id}:a:1"},
                                    "b": {"value": "B",
                                          "op_id": f"{h.run_id}:b:1"}})
        self.assertEqual(len(self.adapter.commits), 2)
        # 终态再次 resume 不产生新效果
        v2 = run(eng.resume(h.run_id))
        self.assertEqual(v2.state, "completed")
        self.assertEqual(len(self.adapter.commits), 2)

    def test_completed_not_redone_after_restart(self):
        eng = self.engine()
        h = run(eng.start("ok"))
        # 全新引擎/“重启”
        eng2 = self.engine()
        views = run(eng2.recover_all())
        self.assertEqual(views, [])  # 没有活动运行
        v = run(eng2.open(h.run_id))
        self.assertEqual(v.state, "completed")
        self.assertEqual(len(self.adapter.commits), 2)


class TestRollback(BaseCase):
    def test_failure_rolls_back_in_reverse_order(self):
        eng = self.engine()
        h = run(eng.start("withcomp"))
        v = run(eng.open(h.run_id))
        self.assertEqual(v.state, "rolled_back")
        states = [s.state for s in v.steps]
        self.assertEqual(states, ["succeeded", "succeeded", "failed"])
        comps = [(c.step_name, c.state) for c in v.compensations]
        self.assertEqual(comps, [("b", "succeeded"), ("a", "succeeded")])
        # 恢复逆序：b 的恢复先于 a 的恢复
        parsed = []
        for cid in self.adapter.commits:
            parts = cid.split(":")
            parsed.append((parts[2], "comp") if parts[1] == "comp"
                          else (parts[1], "fwd"))
        self.assertEqual(
            parsed,
            [("a", "fwd"), ("b", "fwd"), ("b", "comp"), ("a", "comp")],
        )

    def test_comp_failure_blocks_then_resumes(self):
        eng = self.engine()
        h = run(eng.start("withcomp", drive=False))
        rid = h.run_id

        async def scenario():
            task = asyncio.create_task(eng.resume(rid))
            # 等到两个正向 op 都提交后让适配器离线，使恢复动作失败
            while len(self.adapter.commits) < 2:
                await asyncio.sleep(0.005)
            self.adapter.offline = True
            return await task

        v = run(scenario())
        self.assertEqual(v.state, "compensating")
        self.assertTrue(v.is_blocked)
        self.assertEqual(v.compensations[0].state, "unknown")
        # 仪器效果仍是正向两次（无恢复）
        self.assertEqual(len(self.adapter.commits), 2)

        # 适配器恢复，继续同一次运行
        self.adapter.offline = False
        v2 = run(eng.resume(rid))
        self.assertEqual(v2.state, "rolled_back")
        self.assertEqual(len(self.adapter.commits), 4)


class TestStartedCrashRecovery(BaseCase):
    def test_started_step_resolved_committed_via_same_opid(self):
        eng = self.engine()
        h = run(eng.start("ok", drive=False))
        rid = h.run_id
        # 手动制造：a 已在适配器生效，但库里只记录了 started
        # （模拟 mark_started 后、确认前进程被杀）。
        store = eng.store
        run(asyncio.to_thread(
            store.acquire_lease, rid, "old-owner", 0.2
        ))
        run(asyncio.to_thread(
            store.mark_step_started, rid, "old-owner", 1,
            "a", f"{rid}:a:1", 1
        ))
        # 适配器侧：同 op_id 的效果已经存在
        run(self.adapter.op(
            StepContext(run_id=rid, step_name="a", attempt=1,
                        op_id=f"{rid}:a:1"),
            {"v": "A"}, {},
        ))
        self.assertEqual(len(self.adapter.commits), 1)

        # 等旧租约过期，由新引擎接管恢复
        eng2 = self.engine(ttl=0.2)
        import time
        time.sleep(0.3)
        v = run(eng2.resume(rid))
        self.assertEqual(v.state, "completed")
        # adapter.op 对同一 op_id 不重复提交
        self.assertEqual(len(self.adapter.commits), 2)  # a + b
        self.assertEqual(v.step("a").result["op_id"], f"{rid}:a:1")

    def test_started_step_resolved_not_executed_then_rollback(self):
        eng = self.engine(ttl=0.2)
        h = run(eng.start("resolved", drive=False))
        rid = h.run_id
        store = eng.store
        epoch = run(asyncio.to_thread(store.acquire_lease, rid, "o", 0.2))
        # a 成功，b 只 started（适配器没有该 op_id 效果）
        run(asyncio.to_thread(
            store.mark_step_started, rid, "o", epoch,
            "a", f"{rid}:a:1", 1
        ))
        run(self.adapter.op(
            StepContext(rid, "a", 1, f"{rid}:a:1"), {"v": "A"}, {}))
        run(asyncio.to_thread(
            store.resolve_step, rid, "o", epoch, "a", "succeeded",
            result=self.adapter.effects[f"{rid}:a:1"], op_id=f"{rid}:a:1"
        ))
        run(asyncio.to_thread(
            store.mark_step_started, rid, "o", epoch,
            "b", f"{rid}:b:1", 1
        ))
        import time
        time.sleep(0.3)
        v = run(eng.resume(rid))
        # b 确认未执行 -> failed；只有真正生效过的 a 需要恢复
        self.assertEqual(v.state, "rolled_back")
        self.assertEqual(v.step("b").state, "failed")
        comps = {c.step_name: c.state for c in v.compensations}
        self.assertEqual(comps, {"a": "succeeded"})


class TestUnknownBlocks(BaseCase):
    def test_unknown_keeps_run_blocked_and_retryable(self):
        eng = self.engine()
        h = run(eng.start("resolved", drive=False))
        rid = h.run_id

        async def scenario():
            self.adapter.offline = True
            task = asyncio.create_task(eng.resume(rid))
            # a 的 resolver 会一直 Unknown
            await asyncio.sleep(0.1)
            v1 = await task
            return v1

        v1 = run(scenario())
        self.assertTrue(v1.is_blocked)
        self.assertEqual(v1.step("a").state, "unknown")
        self.assertEqual(v1.state, "running")
        # 适配器恢复：resolver 看到没有 op_id 效果 -> NotExecuted -> 失败回滚
        self.adapter.offline = False
        v2 = run(eng.resume(rid))
        self.assertEqual(v2.state, "rolled_back")


class TestCancel(BaseCase):
    def test_cancel_before_start_skips_all(self):
        eng = self.engine()
        h = run(eng.start("ok", drive=False))
        self.assertTrue(run(eng.request_cancel(h.run_id)))
        v = run(eng.resume(h.run_id))
        self.assertEqual(v.state, "cancelled")
        self.assertTrue(all(s.state == "skipped" for s in v.steps))
        self.assertEqual(self.adapter.commits, [])

    def test_cancel_after_effect_compensates(self):
        eng = self.engine()
        h = run(eng.start("withcomp", drive=False))
        rid = h.run_id
        gate = asyncio.Event()

        async def hanging_verify(ctx, p, r):
            await asyncio.wait_for(gate.wait(), timeout=2.0)
            raise StepFailed("x")

        reg2 = make_registry(self.adapter)
        reg2.handler("hang", hanging_verify)
        reg2.define("withcomp", 2, [
            Step("a", "op", params={"v": "A"}, compensation_ref="comp"),
            Step("b", "op", params={"v": "B"}, compensation_ref="comp"),
            Step("v", "hang"),
        ])
        eng2 = Engine(Store(self.db_path), reg2, lease_ttl_seconds=5.0)

        async def scenario():
            task = asyncio.create_task(eng2.resume(rid))
            while len(self.adapter.commits) < 2:
                await asyncio.sleep(0.005)
            await eng2.request_cancel(rid, "user")
            gate.set()
            return await task

        v = run(scenario())
        self.assertEqual(v.state, "cancelled")
        # 两个正向 + 两个恢复
        self.assertEqual(len(self.adapter.commits), 4)


class TestLease(BaseCase):
    def test_concurrent_resume_rejected(self):
        eng = self.engine(ttl=30.0)
        h = run(eng.start("ok", drive=False))
        gate = asyncio.Event()

        async def slow(ctx, p, r):
            await asyncio.wait_for(gate.wait(), timeout=2.0)
            return {"value": "A", "op_id": ctx.op_id}

        reg2 = make_registry(self.adapter)
        reg2.handler("op", slow)
        eng2 = Engine(Store(self.db_path), reg2, lease_ttl_seconds=30.0)

        async def scenario():
            task = asyncio.create_task(eng2.resume(h.run_id))
            await asyncio.sleep(0.1)
            eng3 = self.engine(ttl=30.0)
            with self.assertRaises(RunBusyError):
                await eng3.resume(h.run_id)
            gate.set()
            v = await task
            self.assertEqual(v.state, "completed")

        run(scenario())

    def test_stale_epoch_write_rejected(self):
        eng = self.engine(ttl=0.2)
        h = run(eng.start("ok", drive=False))
        rid = h.run_id
        old_epoch = run(asyncio.to_thread(
            eng.store.acquire_lease, rid, "old", 0.2
        ))
        import time
        time.sleep(0.3)
        eng2 = self.engine(ttl=5.0)
        v = run(eng2.resume(rid))
        self.assertEqual(v.state, "completed")
        with self.assertRaises(LeaseLostError):
            run(asyncio.to_thread(
                eng.store.append_event, rid, "old", old_epoch, "stale"
            ))


class TestVersioning(BaseCase):
    def test_missing_refs_incompatible_then_resume(self):
        eng = self.engine()
        h = run(eng.start("ok", drive=False))
        rid = h.run_id

        upgraded = Registry()
        # 只提供新名字，旧引用缺失
        upgraded.handler("op_v2", self.adapter.op)
        upgraded.define("ok", 2, [Step("a", "op_v2"), Step("b", "op_v2")])

        eng2 = Engine(Store(self.db_path), upgraded, lease_ttl_seconds=1.0)
        with self.assertRaises(IncompatibleRunError) as cm:
            run(eng2.resume(rid))
        self.assertIn("op", cm.exception.missing)
        v = run(eng2.open(rid))
        self.assertEqual(v.state, "incompatible")
        self.assertTrue(v.is_blocked)

        # 兼容版本重新提供旧引用 -> 继续
        compat = self.engine(ttl=1.0)
        v2 = run(compat.resume(rid))
        self.assertEqual(v2.state, "completed")

    def test_terminal_run_idempotent_under_new_engine(self):
        eng = self.engine()
        h = run(eng.start("ok"))
        before = list(self.adapter.commits)
        eng2 = self.engine()
        v = run(eng2.resume(h.run_id))
        self.assertTrue(v.terminal)
        self.assertEqual(self.adapter.commits, before)


class TestCompensationCrash(BaseCase):
    """崩溃发生在补偿阶段：恢复动作 started/unknown 后重启续完。"""

    def _build_rolled_back_midway(self, rid):
        """手工构造：a 已生效、验证失败、comp(a) 已在适配器生效但
        执行库只记录到 started（进程在恢复确认前被杀）。"""

        store = self.engine().store
        epoch = run(asyncio.to_thread(
            store.acquire_lease, rid, "dead", 0.2
        ))
        # 正向 a 成功
        run(asyncio.to_thread(
            store.mark_step_started, rid, "dead", epoch,
            "a", f"{rid}:a:1", 1
        ))
        a_result = {"value": "A", "op_id": f"{rid}:a:1"}
        run(self.adapter.op(
            StepContext(rid, "a", 1, f"{rid}:a:1"), {"v": "A"}, {}))
        run(asyncio.to_thread(
            store.resolve_step, rid, "dead", epoch, "a", "succeeded",
            result=a_result, op_id=f"{rid}:a:1"
        ))
        # 进入补偿阶段
        run(asyncio.to_thread(
            store.begin_compensation, rid, "dead", epoch, "test",
            {"message": "test"}
        ))
        # 恢复动作在适配器已生效，库里只 started
        comp_id = f"{rid}:comp:a:1"
        run(self.adapter.comp(
            StepContext(rid, "a", 1, comp_id, is_compensation=True),
            None, a_result
        ))
        run(asyncio.to_thread(
            store.mark_comp_started, rid, "dead", epoch,
            "a", comp_id, 1
        ))

    def test_comp_started_resumed_with_same_opid(self):
        eng = self.engine(ttl=0.2)
        rid = "comp-crash"
        run(eng.start("withcomp", run_id=rid, drive=False))
        self._build_rolled_back_midway(rid)

        # 适配器侧恢复效果已存在；重启后续完，不重复恢复
        import time
        time.sleep(0.3)
        v = run(eng.resume(rid))
        self.assertEqual(v.state, "rolled_back")
        comp = next(c for c in v.compensations if c.step_name == "a")
        self.assertEqual(comp.state, "succeeded")
        # 恢复 op_id 沿用，适配器不产生第二次效果
        comp_effects = [c for c in self.adapter.commits if ":comp:" in c]
        self.assertEqual(comp_effects, [f"{rid}:comp:a:1"])

    def test_open_step_committed_after_comp_phase_starts_gets_undone(self):
        """进入补偿阶段后才确认一个此前结果不明的步骤：
        它必须补建恢复动作并被撤回，不能漏补偿。"""

        eng = self.engine(ttl=0.2)
        rid = "comp-open-committed"
        run(eng.start("withcomp", run_id=rid, drive=False))
        store = eng.store
        epoch = run(asyncio.to_thread(store.acquire_lease, rid, "dead", 0.2))

        async def commit_forward(step, value):
            op_id = f"{rid}:{step}:1"
            await asyncio.to_thread(
                store.mark_step_started, rid, "dead", epoch, step, op_id, 1
            )
            result = await self.adapter.op(
                StepContext(rid, step, 1, op_id), {"v": value}, {}
            )
            await asyncio.to_thread(
                store.resolve_step, rid, "dead", epoch, step, "succeeded",
                result=result, op_id=op_id
            )

        run(commit_forward("a", "A"))
        # b 只推进到 started 就进入补偿阶段（模拟崩溃/取消后的边界）
        run(asyncio.to_thread(
            store.mark_step_started, rid, "dead", epoch,
            "b", f"{rid}:b:1", 1
        ))
        run(self.adapter.op(
            StepContext(rid, "b", 1, f"{rid}:b:1"), {"v": "B"}, {}
        ))
        run(asyncio.to_thread(
            store.begin_compensation, rid, "dead", epoch, "test",
            {"message": "test"}
        ))

        import time
        time.sleep(0.3)
        v = run(eng.resume(rid))
        self.assertEqual(v.state, "rolled_back")
        # b 在补偿阶段被 resolver 确认为 committed，然后补撤回
        self.assertEqual(v.step("b").state, "succeeded")
        comps = {c.step_name: c.state for c in v.compensations}
        self.assertEqual(comps, {"b": "succeeded", "a": "succeeded"})
        names = []
        for cid in self.adapter.commits:
            parts = cid.split(":")
            names.append((parts[2], "comp") if parts[1] == "comp"
                         else (parts[1], "fwd"))
        self.assertEqual(names, [
            ("a", "fwd"), ("b", "fwd"),
            ("b", "comp"), ("a", "comp"),
        ])


if __name__ == "__main__":
    unittest.main(verbosity=2)
