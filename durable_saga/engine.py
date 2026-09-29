"""异步驱动引擎：把一次运行从持久记录推进到终态或“可恢复的暂停点”。

恢复模型（最重要的语义）
========================

进程可能在任何时刻被杀。库对一步操作只可能看到：

* ``pending`` —— 从未发起，可以安全执行；
* ``started`` —— 已经向控制器发起，但本库没有拿到确认回执。
  此时**禁止**凭猜测把它当失败重来（可能产生第二次外部效果），
  也**禁止**当成功继续后续步骤；必须用同一个 ``op_id`` 去适配器
  重复查询/重试，拿到最初的结果；
* ``unknown`` —— 尝试过确认仍无定论，运行停在 blocked，
  等适配器恢复后再继续。

适配器按约定保证同一 op_id 幂等，因此库与控制器之间不需要
共享数据库事务。
"""

from __future__ import annotations

import asyncio
import inspect
import os
import socket
import uuid
from dataclasses import dataclass
from typing import Any

from .contracts import (
    AmbiguousOutcome,
    Committed,
    Flow,
    NotExecuted,
    Registry,
    Resolution,
    StepContext,
    StepFailed,
    Unknown,
)
from .errors import IncompatibleRunError, RunBusyError
from .store import LeaseLostError, Store, error_json
from .views import TERMINAL_STATES, RunView

_COMP_DONE = frozenset({"succeeded", "not_required", "not_occurred"})
#: 结果不明、恢复时必须先用同一 op_id 核实外部真相的步骤状态。
_OPEN_STATES = frozenset({"started", "unknown"})


@dataclass(frozen=True)
class RunHandle:
    """start 返回的轻量句柄，运行标识在服务重启后仍然可查。"""

    run_id: str

    async def open(self, engine: "Engine") -> RunView:
        return await engine.open(self.run_id)


def _snapshot_flow(flow: Flow) -> list[dict[str, Any]]:
    """把流程定义转成可 JSON 固化的快照（启动时语义即在此冻结）。"""

    import json

    snapshot: list[dict[str, Any]] = []
    for seq, s in enumerate(flow.steps):
        item = {
            "seq": seq,
            "name": s.name,
            "handler_ref": s.handler_ref,
            "compensation_ref": s.compensation_ref,
            "resolver_ref": s.resolver_ref,
            "params": s.params,
            "comp_params": s.comp_params,
        }
        json.dumps(item)  # 提前暴露不可序列化错误
        snapshot.append(item)
    return snapshot


def _missing_refs(registry: Registry, snapshot: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for s in snapshot:
        refs.append(s["handler_ref"])
        if s.get("compensation_ref"):
            refs.append(s["compensation_ref"])
        if s.get("resolver_ref"):
            refs.append(s["resolver_ref"])
    return list(dict.fromkeys(r for r in refs if registry.get_handler(r) is None))


def _results_map(view: RunView) -> dict[str, Any]:
    """此前各步已确认的结果；后续步骤与恢复函数都只能引用这些。"""

    return {
        s.name: s.result
        for s in view.steps
        if s.state == "succeeded" and s.result is not None
    }


class Engine:
    """一次运行的驱动者。多个 Engine 实例可连接同一个 Store 文件。"""

    def __init__(
        self,
        store: Store,
        registry: Registry,
        *,
        lease_ttl_seconds: float = 30.0,
        owner: str | None = None,
    ):
        self.store = store
        self.registry = registry
        self.ttl = float(lease_ttl_seconds)
        self.owner = owner or (
            f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        )

    # ------------------------------------------------------------------ #
    # 对外入口
    # ------------------------------------------------------------------ #

    async def start(
        self,
        flow_name: str,
        *,
        version: int | None = None,
        params: Any = None,
        run_id: str | None = None,
        drive: bool = True,
    ) -> RunHandle:
        """登记一次运行并取得后续可查询的运行标识。

        Args:
            flow_name/version: 注册的流程；version 缺省取最高版本，
                但快照与版本号会固化进运行记录。
            params: 可 JSON 序列化的运行级参数。
            drive: True 时立即驱动到终态或 blocked；False 时只创建，
                由稍后的 resume 或服务重启接手。
        """

        flow = self.registry.get_flow(flow_name, version)
        missing = self.registry.missing_refs(flow)
        if missing:
            raise IncompatibleRunError("<new>", missing)
        snapshot = _snapshot_flow(flow)
        rid = run_id or uuid.uuid4().hex
        await asyncio.to_thread(
            self.store.create_run, rid, flow.name, flow.version, snapshot, params
        )
        handle = RunHandle(rid)
        if drive:
            await self.resume(rid)
        return handle

    async def resume(self, run_id: str) -> RunView:
        """取得租约并推进一次运行，返回终态或 blocked 视图。

        运行正被未过期租约占用时抛 :class:`RunBusyError`——这不是
        失败，稍后重试或等租约过期接管即可。
        """

        view = await self.open(run_id)
        if view.terminal:
            return view
        epoch = await asyncio.to_thread(
            self.store.acquire_lease, run_id, self.owner, self.ttl
        )
        stop = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(run_id, epoch, stop))
        try:
            return await self._drive(run_id, epoch)
        except LeaseLostError:
            # 租约已在推进途中被更新的持有者接管：立即停止，不做任何
            # 进一步外部调用；当前进展由新持有者负责。
            stop.set()
            return await self.open(run_id)
        finally:
            stop.set()
            heartbeat.cancel()
            try:
                await heartbeat
            except BaseException:
                pass
            # 终态在落库时已清空租约；这里仅在仍持有时释放。
            await asyncio.to_thread(
                self.store.release_lease, run_id, self.owner, epoch
            )

    async def request_cancel(self, run_id: str, reason: str = "") -> bool:
        """登记取消意图（外部效果约定）。

        取消 Python 协程不等于控制器已恢复：本方法只把意图持久化。
        若运行正在推进，驱动者会在安全边界停止启动新步骤，并对
        已确认生效的操作执行恢复；也可随后调用 resume 完成撤回。
        """

        return await asyncio.to_thread(self.store.request_cancel, run_id, reason)

    async def cancel(self, run_id: str, reason: str = "") -> RunView:
        """登记取消意图，并在租约空闲时立即驱动恢复。"""

        await self.request_cancel(run_id, reason)
        try:
            return await self.resume(run_id)
        except RunBusyError:
            # 正被原驱动者推进，它会在循环边界看到取消意图。
            return await self.open(run_id)

    async def open(self, run_id: str, *, events: bool = True) -> RunView:
        """只读重开一次运行：步骤/恢复动作/事件全部可独立核对。"""

        return await asyncio.to_thread(self.store.load, run_id, events)

    async def list_runs(self, limit: int = 200) -> list[RunView]:
        return await asyncio.to_thread(self.store.list_runs, limit)

    async def recover_all(self) -> list[RunView]:
        """采集服务重启后调用：驱动所有未结束且租约空闲的运行。

        包括“验证失败但恢复未做完”“控制器故障导致 blocked”的运行。
        """

        ids = await asyncio.to_thread(self.store.active_run_ids)
        views: list[RunView] = []
        for rid in ids:
            try:
                views.append(await self.resume(rid))
            except RunBusyError:
                views.append(await self.open(rid))
        return views

    # ------------------------------------------------------------------ #
    # 租约心跳
    # ------------------------------------------------------------------ #

    async def _heartbeat(self, run_id: str, epoch: int, stop: asyncio.Event) -> None:
        interval = max(self.ttl / 3.0, 0.05)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            ok = await asyncio.to_thread(
                self.store.renew_lease, run_id, self.owner, epoch, self.ttl
            )
            if not ok:
                # 租约已被更新的持有者接管：本协程应尽快退出。
                stop.set()

    # ------------------------------------------------------------------ #
    # 主状态机
    # ------------------------------------------------------------------ #

    async def _drive(self, run_id: str, epoch: int) -> RunView:
        snapshot = await asyncio.to_thread(self.store.get_snapshot, run_id)
        by_name = {s["name"]: s for s in snapshot}

        while True:
            await asyncio.sleep(0)
            view = await asyncio.to_thread(self.store.load, run_id, False)
            if view.state in TERMINAL_STATES:
                return await asyncio.to_thread(self.store.load, run_id)

            # 流程定义来自运行开始时的快照；函数按引用名现解析。
            # 缺失即明确报告无法继续，绝不换成新函数接着跑。
            missing = _missing_refs(self.registry, snapshot)
            if missing:
                if view.state != "incompatible":
                    await asyncio.to_thread(
                        self.store.mark_incompatible, run_id, missing
                    )
                raise IncompatibleRunError(run_id, missing)

            # 此前因缺函数停在 incompatible；函数补齐后恢复到中断阶段。
            if view.state == "incompatible":
                state = await asyncio.to_thread(
                    self.store.clear_incompatible_if_ready,
                    run_id, self.owner, epoch,
                )
                if state == "incompatible":  # 理论上不会（引用刚校验过）
                    raise IncompatibleRunError(run_id, [])
                view = await asyncio.to_thread(
                    self.store.load, run_id, False
                )

            if view.state == "running":
                if await self._phase_forward(run_id, epoch, view, by_name):
                    return await asyncio.to_thread(self.store.load, run_id)
            else:  # compensating
                if await self._phase_compensate(run_id, epoch, view, by_name):
                    return await asyncio.to_thread(self.store.load, run_id)

    # ------------------------------------------------------------------ #
    # 向前推进。返回 True 表示本轮驱动应结束（终态或 blocked）。
    # ------------------------------------------------------------------ #

    async def _phase_forward(self, run_id, epoch, view, by_name) -> bool:
        results = _results_map(view)

        # 1) 任何结果不明的步骤必须先查清外部真相；即使已请求取消，
        #    不知道它有没有生效，就无法决定是否需要撤回。
        for sv in view.steps:
            if sv.state in _OPEN_STATES:
                outcome = await self._resolve_open_step(
                    run_id, epoch, view, by_name[sv.name], sv
                )
                if outcome == "blocked":
                    return True
                if outcome == "not_executed":
                    return await self._begin_compensation(
                        run_id, epoch, view, sv, "step_not_executed"
                    )
                return False  # committed：重新加载并继续评估

        # 2) 防御：已明确失败的步骤（正常在当次循环内即转补偿）。
        for sv in view.steps:
            if sv.state == "failed":
                return await self._begin_compensation(
                    run_id, epoch, view, sv, "step_failed"
                )

        # 3) 取消请求：尚未执行的校准不能继续，直接进入恢复。
        if view.cancel_requested:
            return await self._begin_compensation(
                run_id, epoch, view, None, "cancelled"
            )

        # 4) 执行下一个待办步骤。
        pending = next((s for s in view.steps if s.state == "pending"), None)
        if pending is None:
            await asyncio.to_thread(
                self.store.complete_run, run_id, self.owner, epoch, results
            )
            return True

        outcome = await self._attempt_step(
            run_id, epoch, view, by_name[pending.name], pending, results
        )
        return outcome  # True=blocked 结束；False=已落盘，循环继续

    async def _begin_compensation(
        self, run_id, epoch, view, failed_step, reason: str
    ) -> bool:
        failure = None
        if failed_step is not None:
            failure = failed_step.error or {"message": reason,
                                            "step": failed_step.name}
        else:
            failure = {"message": reason}
        await asyncio.to_thread(
            self.store.begin_compensation, run_id, self.owner, epoch,
            reason, failure,
        )
        return False  # 状态已转 compensating，外层循环继续

    # ------------------------------------------------------------------ #
    # 单个步骤的执行与确认
    # ------------------------------------------------------------------ #

    def _ctx(self, view, step_name: str, attempt: int, op_id: str,
             is_comp: bool = False) -> StepContext:
        return StepContext(
            run_id=view.run_id, step_name=step_name, attempt=attempt,
            op_id=op_id, is_compensation=is_comp,
            flow_name=view.flow_name, flow_version=view.flow_version,
            run_params=view.run_params,
        )

    async def _call(self, fn, ctx: StepContext, params, results):
        """统一调用：支持 async def 与同步函数。"""

        rv = fn(ctx, params, results)
        if inspect.isawaitable(rv):
            rv = await rv
        return rv
    async def _attempt_step(
        self, run_id, epoch, view, spec, sv, results
    ) -> bool:
        """执行一次处于 pending 的步骤，并落确定状态。返回是否结束。"""

        attempt = 1
        op_id = f"{run_id}:{sv.name}:{attempt}"
        ctx = self._ctx(view, sv.name, attempt, op_id)
        await asyncio.to_thread(
            self.store.mark_step_started, run_id, self.owner, epoch,
            sv.name, op_id, attempt,
        )

        try:
            rv = await self._call(
                self.registry.get_handler(spec["handler_ref"]),
                ctx, spec.get("params"), results,
            )
        except StepFailed as e:
            # 明确的业务失败（如验证超差），没有需要撤回的本步效果。
            await asyncio.to_thread(
                self.store.resolve_step, run_id, self.owner, epoch, sv.name,
                "failed", error=error_json(e),
            )
            return False
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - 进程活着也可能抛不明异常
            # 不直接当失败：先按同一 op_id 确认一次控制器实际状态。
            rv = AmbiguousOutcome(reason=f"handler raised {type(e).__name__}",
                                  detail=error_json(e))

        if isinstance(rv, AmbiguousOutcome):
            outcome = await self._confirm(
                run_id, epoch, view, spec, sv.name, op_id, attempt,
                results, why=rv.reason, detail=rv.detail,
            )
            return outcome
        if isinstance(rv, Resolution):
            outcome = await self._apply_resolution(
                run_id, epoch, view, spec, sv.name, op_id, attempt, rv
            )
            return outcome == "blocked"

        await asyncio.to_thread(
            self.store.resolve_step, run_id, self.owner, epoch, sv.name,
            "succeeded", result=rv, op_id=op_id,
        )
        return False

    async def _resolve_open_step(self, run_id, epoch, view, spec, sv) -> str:
        """处理崩溃/重启后看到的 started|unknown 步骤。

        绝不重新发起一次新操作：优先用 resolver 查询，否则用同一个
        op_id 重试处理函数（适配器按 op_id 去重，返回最初结果）。
        """

        attempt = max(sv.attempt or 0, 1)
        op_id = sv.op_id or f"{run_id}:{sv.name}:{attempt}"
        results = _results_map(view)
        outcome = await self._confirm(
            run_id, epoch, view, spec, sv.name, op_id, attempt, results,
            why=f"recover {sv.state} after restart",
        )
        return outcome  # committed / not_executed / blocked

    async def _confirm(
        self, run_id, epoch, view, spec, step_name, op_id, attempt,
        results, *, why: str, detail: Any = None,
    ) -> str:
        """用同一 op_id 确认一次结果不明的操作。"""

        resolver_ref = spec.get("resolver_ref")
        ctx = self._ctx(view, step_name, attempt, op_id)

        if resolver_ref:
            try:
                res = await self._call(
                    self.registry.get_handler(resolver_ref),
                    ctx, spec.get("params"), results,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                res = Unknown(reason=f"resolver raised {type(e).__name__}: {e}")
            return await self._apply_resolution(
                run_id, epoch, view, spec, step_name, op_id, attempt, res
            )

        # 没有 resolver：按约定用同一 op_id 重试处理函数（仅一次）。
        try:
            rv = await self._call(
                self.registry.get_handler(spec["handler_ref"]),
                ctx, spec.get("params"), results,
            )
        except StepFailed as e:
            await asyncio.to_thread(
                self.store.resolve_step, run_id, self.owner, epoch, step_name,
                "failed", error=error_json(e), op_id=op_id, attempt=attempt,
            )
            return "not_executed"
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            rv = None
            await asyncio.to_thread(
                self.store.resolve_step, run_id, self.owner, epoch, step_name,
                "unknown", error=error_json(e), op_id=op_id, attempt=attempt,
            )
            await self._block(run_id, epoch,
                              f"无法确认步骤 {step_name} 是否已生效"
                              f"（op_id={op_id}）", step_name,
                              {"why": why, "detail": detail})
            return "blocked"

        if isinstance(rv, AmbiguousOutcome) or isinstance(rv, Unknown):
            reason = getattr(rv, "reason", why)
            await asyncio.to_thread(
                self.store.resolve_step, run_id, self.owner, epoch, step_name,
                "unknown", error={"reason": reason, "detail": detail},
                op_id=op_id, attempt=attempt,
            )
            await self._block(run_id, epoch,
                              f"无法确认步骤 {step_name} 是否已生效"
                              f"（op_id={op_id}）", step_name,
                              {"why": why, "detail": detail})
            return "blocked"
        if isinstance(rv, NotExecuted):
            await asyncio.to_thread(
                self.store.resolve_step, run_id, self.owner, epoch, step_name,
                "failed", error={"reason": "适配器确认操作未执行"},
                op_id=op_id, attempt=attempt,
            )
            return "not_executed"
        if isinstance(rv, Committed):
            result = rv.result
        else:
            result = rv
        await asyncio.to_thread(
            self.store.resolve_step, run_id, self.owner, epoch, step_name,
            "succeeded", result=result, op_id=op_id, attempt=attempt,
        )
        return "committed"

    async def _apply_resolution(
        self, run_id, epoch, view, spec, step_name, op_id, attempt, res
    ) -> str:
        """把 resolver 的结论落盘，返回 committed/not_executed/blocked。"""

        if isinstance(res, Committed):
            await asyncio.to_thread(
                self.store.resolve_step, run_id, self.owner, epoch, step_name,
                "succeeded", result=res.result, op_id=op_id, attempt=attempt,
            )
            return "committed"
        if isinstance(res, NotExecuted):
            await asyncio.to_thread(
                self.store.resolve_step, run_id, self.owner, epoch, step_name,
                "failed", error={"reason": "resolver 确认操作未执行"},
                op_id=op_id, attempt=attempt,
            )
            return "not_executed"
        # Unknown 或 resolver 返回了不支持的类型
        reason = res.reason if isinstance(res, Unknown) else f"未知结论 {type(res)}"
        await asyncio.to_thread(
            self.store.resolve_step, run_id, self.owner, epoch, step_name,
            "unknown", error={"reason": reason}, op_id=op_id, attempt=attempt,
        )
        await self._block(run_id, epoch,
                          f"步骤 {step_name} 结果不明，等待确认"
                          f"（op_id={op_id}）", step_name, {"reason": reason})
        return "blocked"

    async def _block(self, run_id, epoch, reason, step_name, detail=None) -> None:
        await asyncio.to_thread(
            self.store.mark_blocked, run_id, self.owner, epoch,
            reason, step_name, detail,
        )

    # ------------------------------------------------------------------ #
    # 补偿阶段：按依赖逆序撤回已确认生效的操作
    # ------------------------------------------------------------------ #

    async def _phase_compensate(self, run_id, epoch, view, by_name) -> bool:
        results = _results_map(view)

        # 补偿过程中若发现某步其实仍不明（极少见：崩溃点横跨转阶段），
        # 同样必须先确认。
        for sv in view.steps:
            if sv.state in _OPEN_STATES:
                outcome = await self._resolve_open_step(
                    run_id, epoch, view, by_name[sv.name], sv
                )
                if outcome == "blocked":
                    return True
                if outcome == "not_executed":
                    # 该步没生效：确保其恢复动作记为无需发生。
                    await self._note_not_occurred(run_id, epoch, sv.name)
                elif outcome == "committed":
                    # 跨越补偿阶段才确认生效：确保它会被撤回。
                    await asyncio.to_thread(
                        self.store.ensure_compensation_for_step,
                        run_id, self.owner, epoch, sv.name,
                    )
                return False

        # 找到下一个未完成的恢复动作（seq 逆序即依赖逆序）。
        todo = [c for c in view.compensations if c.state not in _COMP_DONE]
        if not todo:
            cancelled = view.cancel_requested
            failure = view.failure
            await asyncio.to_thread(
                self.store.finish_rollback, run_id, self.owner, epoch,
                cancelled, failure,
            )
            return True

        comp = todo[0]
        outcome = await self._attempt_comp(
            run_id, epoch, view, by_name[comp.step_name], comp, results
        )
        return outcome

    async def _note_not_occurred(self, run_id, epoch, step_name) -> None:
        await asyncio.to_thread(
            self.store.ensure_comp_not_occurred, run_id, self.owner, epoch,
            step_name,
        )

    async def _attempt_comp(self, run_id, epoch, view, spec, comp, results) -> bool:
        """执行一个恢复动作。返回是否结束本轮（blocked）。"""

        step_name = comp.step_name
        attempt = max(comp.attempt or 0, 1)
        op_id = f"{run_id}:comp:{step_name}:{attempt}"
        ctx = self._ctx(view, step_name, attempt, op_id, is_comp=True)
        await asyncio.to_thread(
            self.store.mark_comp_started, run_id, self.owner, epoch,
            step_name, op_id, attempt,
        )

        step_result = results.get(step_name)
        try:
            rv = await self._call(
                self.registry.get_handler(comp.compensation_ref),
                ctx, spec.get("comp_params"), step_result,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            # 恢复失败不能让仪器状态“假装已恢复”：用同一 op_id 再确认
            # 一次；仍不明则 blocked，等适配器恢复后继续本次运行。
            await asyncio.to_thread(
                self.store.resolve_comp, run_id, self.owner, epoch,
                step_name, "unknown", error=error_json(e),
            )
            await self._block(
                run_id, epoch,
                f"恢复动作 {step_name} 未能确认完成（op_id={op_id}）；"
                f"仪器可能尚未恢复到该步骤之前的状态",
                step_name, error_json(e),
            )
            return True

        if isinstance(rv, AmbiguousOutcome) or isinstance(rv, Unknown):
            reason = getattr(rv, "reason", "恢复结果不明")
            await asyncio.to_thread(
                self.store.resolve_comp, run_id, self.owner, epoch,
                step_name, "unknown", error={"reason": reason},
            )
            await self._block(
                run_id, epoch,
                f"恢复动作 {step_name} 结果不明（op_id={op_id}）；"
                f"仪器可能尚未恢复到该步骤之前的状态",
                step_name, {"reason": reason},
            )
            return True

        result = rv.result if isinstance(rv, Committed) else rv
        await asyncio.to_thread(
            self.store.resolve_comp, run_id, self.owner, epoch,
            step_name, "succeeded", result=result,
        )
        return False
