"""数据模型与状态常量。

所有状态均为字符串常量，直接存入 SQLite；状态取值一旦发布即视为存储格式，
不要随意改动已有取值的含义。

一次运行的状态层次::

    runs.status          整次运行的总体进展（前向 / 撤回 / 终态）
    steps.execute_status 每个校准步骤的执行情况（互不相同、可独立核对）
    steps.compensate_status 每个已生效步骤的撤回情况
    attempts.status      对控制器的*单次*外部尝试（started 是关键的崩溃事实）
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Dict, List, Optional


# ---------------------------------------------------------------------------
# 运行级状态
# ---------------------------------------------------------------------------

RUN_PENDING = "pending"
"""已登记、尚未取得执行租约。"""

RUN_RUNNING = "running"
"""前向执行中（含正在处理某一步）。"""

RUN_AWAITING_CONFIRMATION = "awaiting_confirmation"
"""某次外部调用结果不明（如进程在记录成功前被杀），需用原 op_id 查询后才能继续。"""

RUN_COMPENSATING = "compensating"
"""验证失败 / 确定失败 / 取消引发的撤回进行中。"""

RUN_COMPENSATION_BLOCKED = "compensation_blocked"
"""撤回动作本身结果不明或未完成，等待适配器恢复后继续。"""

RUN_SUCCEEDED = "succeeded"
"""终态：全部步骤成功。重启后不会再写入、也不会被撤回。"""

RUN_COMPENSATED = "compensated"
"""终态：所有已生效步骤均已撤回，仪器回到原参数。"""

RUN_CANCELLED = "cancelled"
"""终态：取消时没有任何已生效的外部操作需要撤回。"""

RUN_TERMINAL = frozenset(
    {RUN_SUCCEEDED, RUN_COMPENSATED, RUN_CANCELLED}
)

# ---------------------------------------------------------------------------
# 步骤执行状态（execute_status）
# ---------------------------------------------------------------------------

STEP_PENDING = "pending"
STEP_STARTED = "started"
STEP_SUCCEEDED = "succeeded"
STEP_REJECTED = "rejected"
"""处理函数明确报告操作被拒绝，确定无外部效果。"""
STEP_INCONCLUSIVE = "inconclusive"
"""结果不明：控制器可能已生效，等待用原 op_id 查询确认。"""
STEP_SKIPPED = "skipped"
"""进入撤回流程后，尚未执行的前向步骤标记为跳过。"""

# ---------------------------------------------------------------------------
# 步骤撤回状态（compensate_status）
# ---------------------------------------------------------------------------

COMP_NOT_NEEDED = "not_needed"
"""该步骤没有产生需要撤回的外部效果，或没有登记恢复函数。"""
COMP_PENDING = "pending"
COMP_STARTED = "started"
COMP_SUCCEEDED = "succeeded"
COMP_INCONCLUSIVE = "inconclusive"

# ---------------------------------------------------------------------------
# 单次外部尝试状态（attempts.status）
# ---------------------------------------------------------------------------

ATTEMPT_STARTED = "started"
ATTEMPT_SUCCEEDED = "succeeded"
ATTEMPT_REJECTED = "rejected"
ATTEMPT_INCONCLUSIVE = "inconclusive"

ATTEMPT_KIND_EXEC = "execute"
ATTEMPT_KIND_COMP = "compensate"


def compensate_when_field_equals(field: str, equals: Any) -> Dict[str, Any]:
    """内置补偿判据：步骤确认结果中的 ``field`` 等于 ``equals`` 时需要撤回。

    例如验证步骤返回 ``{"passed": False}``，则登记
    ``compensate_when=compensate_when_field_equals("passed", False)``。
    判据以 JSON 形式固化进运行快照，将来恢复时按当时的含义解释，
    不依赖当前代码里的函数对象。
    """

    return {"type": "field_equals", "field": field, "equals": equals}


@dataclasses.dataclass(frozen=True)
class FlowStep:
    """一个有序的校准步骤定义。

    :param key: 步骤在本流程内的唯一标识，后续步骤通过它引用结果。
    :param handler: 执行处理函数名，或直接传入 async 函数（取 ``__name__``）。
    :param title: 人类可读说明。
    :param params: 可保存为 JSON 的静态参数，运行开始时固化。
    :param compensator: 可选的恢复函数名 / async 函数；只能使用已确认的执行结果。
    :param compensate_when: 可选的补偿判据（见
        :func:`compensate_when_field_equals`）；缺省表示该步骤本身永不触发撤回，
        只在其它步骤触发时被连带撤回。
    """

    key: str
    handler: Any
    title: str = ""
    params: Optional[Dict[str, Any]] = None
    compensator: Optional[Any] = None
    compensate_when: Optional[Dict[str, Any]] = None

    def handler_name(self) -> str:
        return _callable_name(self.handler)

    def compensator_name(self) -> Optional[str]:
        return _callable_name(self.compensator) if self.compensator is not None else None


def _callable_name(value: Any) -> str:
    if isinstance(value, str):
        return value
    name = getattr(value, "__name__", None)
    if not name:
        raise TypeError(f"处理函数必须是字符串名称或已定义函数，收到 {value!r}")
    return name


@dataclasses.dataclass(frozen=True)
class Flow:
    """调用方登记的有序校准流程。"""

    name: str
    version: int
    steps: List[FlowStep]
    title: str = ""

    @classmethod
    def define(
        cls,
        name: str,
        steps: List[FlowStep],
        *,
        version: int = 1,
        title: str = "",
    ) -> "Flow":
        return cls(name=name, version=version, steps=list(steps), title=title)


# ---------------------------------------------------------------------------
# 查询视图
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class AttemptSnapshot:
    id: int
    step_key: str
    kind: str  # execute / compensate
    op_id: str
    status: str
    result: Optional[Any]
    error: Optional[Dict[str, Any]]
    started_at: str
    finished_at: Optional[str]
    attempt_no: int


@dataclasses.dataclass
class StepSnapshot:
    seq: int
    key: str
    title: str
    handler: str
    params: Optional[Any]
    compensator: Optional[str]
    compensate_when: Optional[Any]
    execute_status: str
    execute_attempt: Optional[AttemptSnapshot]
    result: Optional[Any]
    error: Optional[Dict[str, Any]]
    compensate_status: str
    compensate_attempt: Optional[AttemptSnapshot]
    compensate_error: Optional[Dict[str, Any]]


@dataclasses.dataclass
class FactSnapshot:
    """append-only 的执行事实，顺序号可用于独立核对每一次状态变化。"""

    seq: int
    at: str
    kind: str
    step_key: Optional[str]
    attempt_id: Optional[int]
    detail: Optional[Any]


@dataclasses.dataclass
class RunSnapshot:
    run_id: str
    flow_name: str
    flow_version: int
    flow_title: str
    status: str
    input: Optional[Any]
    error: Optional[Dict[str, Any]]
    blocked_reason: Optional[Dict[str, Any]]
    cancel_requested: bool
    created_at: str
    updated_at: str
    steps: List[StepSnapshot]
    facts: List[FactSnapshot]

    # -- 便于调用方直接使用的派生属性 --------------------------------------

    @property
    def terminal(self) -> bool:
        return self.status in RUN_TERMINAL

    def result(self, step_key: str) -> Any:
        """取某一步**已确认**的执行结果；不存在或未确认时抛出 KeyError。"""

        for step in self.steps:
            if step.key == step_key:
                if step.execute_status != STEP_SUCCEEDED or step.result is None:
                    raise KeyError(
                        f"步骤 {step_key!r} 尚未确认成功（当前 {step.execute_status}）"
                    )
                return _json_sentinel_to_none(step.result)
        raise KeyError(f"未知步骤 {step_key!r}")

    def facts_of(self, kind: Optional[str] = None) -> List[FactSnapshot]:
        if kind is None:
            return list(self.facts)
        return [f for f in self.facts if f.kind == kind]


def _json_sentinel_to_none(value: Any) -> Any:
    # 存储层用 {"__null__": true} 表示 JSON null 与 SQL NULL 的区别；
    # 视图层还原回 Python None。
    if isinstance(value, dict) and value.get("__null__") is True and len(value) == 1:
        return None
    return value


HandlerFn = Callable[..., Any]
"""处理函数签名: ``async def fn(ctx, params) -> JSON 可序列化结果``。

恢复函数签名: ``async def fn(ctx, params, result)``，其中 ``result`` 是
对应执行步骤**已经确认**的结果（恢复原参数所需内容只能来自这里）。
"""
