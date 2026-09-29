"""从持久记录重建出来的只读视图。

调用方（采集服务、排障工具）通过这些视图独立核对：
每个步骤、每个恢复动作各自的状态、结果、失败信息与操作身份，
而不是只拿到一个运行级的成功/失败布尔值。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: 运行总体状态机：
#:   running      正常向前推进（含“暂时 blocked”的情况，见 is_blocked）
#:   completed    所有步骤成功，最终结果已固化（终态）
#:   compensating 正在按依赖逆序撤回已生效操作
#:   rolled_back  全部需要撤回的操作处理完毕（终态）
#:   cancelled    调用方请求取消且外部效果已全部撤回（终态）
#:   incompatible 此刻缺少运行开始时对应的处理函数；补齐兼容函数后
#:                 可以 resume，因此是可恢复状态，不是永久终态。
TERMINAL_STATES = frozenset({"completed", "rolled_back", "cancelled"})
ACTIVE_STATES = frozenset({"running", "compensating", "incompatible"})

#: 步骤/恢复动作状态：
#:   pending   尚未开始
#:   started   已对外部发起，但本进程未确认（进程可能在此间被杀）
#:   unknown   结果不明，恢复时仍无法确认（运行因此 blocked）
#:   succeeded 已拿到适配器确认的成功回执
#:   failed    明确失败，没有需要撤回的外部效果
#:   skipped   因前序失败/取消而未执行
STEP_OPEN_STATES = frozenset({"started", "unknown"})

#: 恢复动作状态（除上述约定外另有两个专属值）：
#:   not_required 该步骤无需撤回（纯读取/验证）
#:   not_occurred 解析确认该步骤当时未真正生效，无需撤回
COMP_NOT_DONE = frozenset({"pending", "started", "unknown"})


@dataclass(frozen=True)
class StepView:
    name: str
    seq: int
    state: str
    handler_ref: str
    compensation_ref: str | None
    resolver_ref: str | None
    params: Any
    result: Any
    error: Any
    op_id: str | None
    attempt: int

    @property
    def needs_attention(self) -> bool:
        """该步骤是否处于结果不明、需要恢复确认的状态。"""

        return self.state in STEP_OPEN_STATES


@dataclass(frozen=True)
class CompensationView:
    step_name: str
    seq: int
    state: str
    compensation_ref: str
    op_id: str | None
    result: Any
    error: Any
    attempt: int

    @property
    def needs_attention(self) -> bool:
        return self.state in COMP_NOT_DONE


@dataclass(frozen=True)
class EventView:
    seq: int
    at: str
    kind: str
    step_name: str | None
    payload: Any


@dataclass(frozen=True)
class RunView:
    run_id: str
    flow_name: str
    flow_version: int
    state: str
    is_blocked: bool
    blocked_reason: str | None
    cancel_requested: bool
    result: Any
    failure: Any
    run_params: Any
    created_at: str
    updated_at: str
    lease_owner: str | None
    lease_epoch: int
    lease_expires_at: str | None
    steps: tuple[StepView, ...] = field(default_factory=tuple)
    compensations: tuple[CompensationView, ...] = field(default_factory=tuple)
    events: tuple[EventView, ...] = field(default_factory=tuple)

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def step(self, name: str) -> StepView | None:
        return next((s for s in self.steps if s.name == name), None)

    def facts(self) -> list[dict[str, Any]]:
        """按时间顺序返回只追加事件流水（“执行事实”）。"""

        return [
            {
                "seq": e.seq,
                "at": e.at,
                "kind": e.kind,
                "step": e.step_name,
                "payload": e.payload,
            }
            for e in self.events
        ]
