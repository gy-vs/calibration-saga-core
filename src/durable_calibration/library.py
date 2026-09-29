"""库门面：采集服务直接嵌入使用的唯一入口。

典型用法::

    lib = CalibrationLibrary("./data/calibration.db")
    lib.register_flow(flow)
    lib.register_handlers({...})

    run_id = await lib.start_run("instrument-calibration", 1,
                                run_input={...})
    snap = await lib.run(run_id)            # 驱动到终态或阻塞

    # 进程重启后（新的库实例、同一个 db 文件）：
    snap = await lib.get_run(run_id)        # 只读查看执行事实
    snap = await lib.resume(run_id)         # 用适配器幂等约定继续/完成恢复
"""

from __future__ import annotations

import json
from typing import Any, Awaitable, Callable, Dict, List, Optional

from . import models as m
from .engine import drive_run
from .errors import (
    FlowVersionMismatch,
    LibraryProgrammingError,
    UnknownFlow,
    UnknownRun,
)
from .storage import Database, ensure_jsonable

HandlerDict = Dict[str, Callable[..., Awaitable[Any]]]


class CalibrationLibrary:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.db = Database(db_path)
        self._flows: Dict[str, Dict[int, m.Flow]] = {}
        self._handlers: HandlerDict = {}

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def close(self) -> None:
        self.db.close()

    async def __aenter__(self) -> "CalibrationLibrary":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # 登记
    # ------------------------------------------------------------------

    def register_handlers(
        self,
        handlers: Optional[HandlerDict] = None,
        **kwargs: Callable[..., Awaitable[Any]],
    ) -> None:
        """登记处理函数/恢复函数。可以传映射，也可以用关键字传入。"""

        merged: HandlerDict = {}
        if handlers:
            for key, value in handlers.items():
                merged[key if isinstance(key, str) else value.__name__] = value
        for key, value in kwargs.items():
            merged[key] = value
        for name, fn in merged.items():
            if not callable(fn):
                raise LibraryProgrammingError(f"{name!r} 不可调用")
            self._handlers[name] = fn

    def register_flow(self, flow: m.Flow) -> None:
        if not flow.steps:
            raise LibraryProgrammingError(f"流程 {flow.name!r} 至少需要一个步骤")
        seen = set()
        for step in flow.steps:
            if not step.key or step.key in seen:
                raise LibraryProgrammingError(
                    f"流程 {flow.name!r} 内步骤 key 重复或为空：{step.key!r}")
            seen.add(step.key)
            if step.handler_name() not in self._handlers:
                raise LibraryProgrammingError(
                    f"流程 {flow.name!r} 步骤 {step.key!r} 的处理函数 "
                    f"{step.handler_name()!r} 尚未登记，请先 register_handlers")
            comp = step.compensator_name()
            if comp is not None and comp not in self._handlers:
                raise LibraryProgrammingError(
                    f"流程 {flow.name!r} 步骤 {step.key!r} 的恢复函数 "
                    f"{comp!r} 尚未登记，请先 register_handlers")
            if step.compensate_when is not None:
                _validate_predicate(step.compensate_when)

        definition = self._definition_snapshot(flow)
        existing = self._flows.setdefault(flow.name, {})
        if flow.version in existing:
            old_snapshot = self._definition_snapshot(existing[flow.version])
            if old_snapshot != definition:
                raise FlowVersionMismatch(
                    f"流程 {flow.name} v{flow.version} 已以不同定义登记："
                    "同一版本号的步骤含义不允许改变，请使用新版本号。"
                )
        existing[flow.version] = flow
    # ------------------------------------------------------------------
    # 运行
    # ------------------------------------------------------------------

    async def start_run(
        self,
        flow_name: str,
        *,
        flow_version: Optional[int] = None,
        run_input: Any = None,
        run_id: Optional[str] = None,
    ) -> str:
        """登记一次运行，返回后续仍可查询的运行标识。

        步骤的 ``params`` 若含 ``{"$from_run_input": "a.b"}`` 占位符，会在
        登记时从 ``run_input`` 中解析替换（支持点路径）。这样同一份流程定义
        可以服务多次运行，而每个步骤固化的参数仍然是纯 JSON。
        """

        flow = self._get_flow(flow_name, flow_version)
        ensure_jsonable(run_input, what="运行入参")
        resolved_steps = [
            m.FlowStep(
                key=s.key,
                handler=s.handler,
                title=s.title,
                params=_resolve_step_params(s.params, run_input),
                compensator=s.compensator,
                compensate_when=s.compensate_when,
            )
            for s in flow.steps
        ]
        for step in resolved_steps:
            ensure_jsonable(step.params, what=f"步骤 {step.key!r} 参数")
        definition = self._definition_snapshot_with_steps(flow, resolved_steps)
        return self.db.insert_run(
            flow_name=flow.name,
            flow_version=flow.version,
            flow_title=flow.title,
            definition=definition,
            run_input=run_input,
            steps=resolved_steps,
            run_id=run_id,
        )

    async def run(self, run_id: str, *, ttl_seconds: float = 30.0,
                  max_replays: int = 3) -> m.RunSnapshot:
        """驱动一次运行到终态或阻塞（启动新运行后的常用入口）。"""

        return await drive_run(
            self.db, run_id, self._handlers,
            ttl_seconds=ttl_seconds, max_replays=max_replays,
        )

    async def resume(
        self,
        run_id: str,
        *,
        ttl_seconds: float = 30.0,
        force: bool = False,
        max_replays: int = 3,
    ) -> m.RunSnapshot:
        """重开原来的运行：确认不明操作、继续前向或完成未结束的恢复。

        ``force=True`` 表示调用方确认原进程已经退出；仍需旧租约过期，
        接管时 fencing epoch 递增，旧进程即使残留也无法再写入。
        """

        return await drive_run(
            self.db, run_id, self._handlers,
            ttl_seconds=ttl_seconds, force=force, max_replays=max_replays,
        )

    async def request_cancel(self, run_id: str) -> None:
        """登记取消请求。取消协程不等于仪器已回原参数。

        若需要等待取消的外部效果完成，调用后继续 :meth:`resume` 驱动，
        直到运行进入 ``compensated`` / ``cancelled``。
        """

        self.db.request_cancel(run_id)

    # ------------------------------------------------------------------
    # 查询（只读，不需要租约）
    # ------------------------------------------------------------------

    async def get_run(self, run_id: str) -> m.RunSnapshot:
        snap = self.db.load_snapshot(run_id)
        if snap is None:
            raise UnknownRun(run_id)
        return snap

    async def list_runs(self) -> List[str]:
        return self.db.list_run_ids()

    def load_flow_definition(self, run_id: str) -> Dict[str, Any]:
        """查看某次运行开始时固化的流程定义（旧运行对应旧步骤含义）。"""

        return self.db.load_definition(run_id)

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _get_flow(self, name: str, version: Optional[int]) -> m.Flow:
        versions = self._flows.get(name)
        if not versions:
            raise UnknownFlow(f"流程 {name!r} 未登记")
        if version is None:
            version = max(versions)
        flow = versions.get(version)
        if flow is None:
            raise UnknownFlow(f"流程 {name!r} 没有版本 {version}")
        return flow

    @staticmethod
    def _definition_snapshot(flow: m.Flow) -> Dict[str, Any]:
        return CalibrationLibrary._definition_snapshot_with_steps(flow, flow.steps)

    @staticmethod
    def _definition_snapshot_with_steps(
        flow: m.Flow, steps: List[m.FlowStep]
    ) -> Dict[str, Any]:
        return {
            "name": flow.name,
            "version": flow.version,
            "title": flow.title,
            "steps": [
                {
                    "seq": seq,
                    "key": s.key,
                    "title": s.title,
                    "handler": s.handler_name(),
                    "params": s.params,
                    "compensator": s.compensator_name(),
                    "compensate_when": s.compensate_when,
                }
                for seq, s in enumerate(steps)
            ],
        }


def _resolve_step_params(params: Any, run_input: Any) -> Any:
    """递归替换 ``{"$from_run_input": "a.b"}`` 占位符。"""

    if isinstance(params, dict):
        if set(params.keys()) == {"$from_run_input"}:
            return _dig(run_input, str(params["$from_run_input"]))
        return {k: _resolve_step_params(v, run_input) for k, v in params.items()}
    if isinstance(params, list):
        return [_resolve_step_params(v, run_input) for v in params]
    return params


def _dig(run_input: Any, dotted: str) -> Any:
    current = run_input
    for part in dotted.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            raise LibraryProgrammingError(
                f"步骤参数占位符 {dotted!r} 无法在运行入参中解析")
    return current


def _validate_predicate(predicate: Any) -> None:
    if not isinstance(predicate, dict) or "type" not in predicate:
        raise LibraryProgrammingError(
            f"补偿判据必须是内置判据（JSON 结构），收到 {predicate!r}")
    ptype = predicate["type"]
    if ptype == "field_equals":
        if not isinstance(predicate.get("field"), str):
            raise LibraryProgrammingError("field_equals 判据需要字符串 field")
        try:
            json.dumps(predicate.get("equals"))
        except (TypeError, ValueError) as exc:
            raise LibraryProgrammingError(
                f"field_equals 判据的期望值不能 JSON 化：{exc}") from exc
        return
    raise LibraryProgrammingError(f"不支持的补偿判据类型：{ptype!r}")
