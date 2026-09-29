"""调用方可见的流程定义与处理函数约定。

处理函数（handler）、恢复函数（compensation）、不确定状态解析函数
（resolver）都是普通的可调用对象，可以是 ``async def`` 或同步函数；
它们的返回值与参数都必须是可保存为 JSON 的数据。

外部效果约定（关键）
--------------------
处理函数必须以 ``ctx.op_id`` 作为操作身份对适配器发起操作，并保证：

* 同一 ``op_id`` 的重复提交/查询返回的是**同一次**操作的最初结果，
  不会产生第二次外部效果（幂等键约定）；
* 进程在记录成功之前被杀死后，库用同一个 ``op_id`` 重新调用处理
  函数或 resolver，即可确认操作到底有没有生效。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .errors import FlowValidationError

#: 处理函数：上下文、本步参数、此前各步已确认结果。
HandlerFn = Callable[["StepContext", Any, dict[str, Any]], Any]
#: 恢复函数：上下文、恢复参数、本步已确认结果。必须足以把仪器撤回。
CompensationFn = Callable[["StepContext", Any, Any], Any]
#: 不确定状态解析函数：返回 Committed/NotExecuted/Unknown。
ResolverFn = Callable[["StepContext", Any, dict[str, Any]], "Resolution"]


@dataclass(frozen=True)
class StepContext:
    """传给处理函数 / 恢复函数 / 解析函数的运行时上下文。"""

    run_id: str
    step_name: str
    attempt: int
    op_id: str
    #: 是否处于补偿阶段（恢复函数里恒为 True）。
    is_compensation: bool = False
    flow_name: str = ""
    flow_version: int = 1
    #: start 时传入的运行级参数（每次运行可不同，不固化进流程定义）。
    run_params: Any = None


@dataclass(frozen=True)
class Step:
    """一个有序校准步骤的静态定义。

    Attributes:
        name: 步骤名，在同一流程内唯一，也是结果字典的键。
        handler_ref: 处理函数在注册表中的引用名。运行记录里保存该
            引用名与流程快照；重启后必须解析到同名函数。
        params: 可 JSON 序列化的静态参数，随运行快照一起固化。
        compensation_ref: 恢复函数引用名；纯读取/验证步骤可省略。
        comp_params: 传给恢复函数的静态参数。恢复真正需要的原参数
            还可以（通常应该）来自前一步的已确认结果。
        resolver_ref: 处理函数结果不明时的解析函数引用名。
    """

    name: str
    handler_ref: str
    params: Any = None
    compensation_ref: str | None = None
    comp_params: Any = None
    resolver_ref: str | None = None


@dataclass(frozen=True)
class Flow:
    """一个命名且带版本的有序步骤流程。"""

    name: str
    version: int
    steps: tuple[Step, ...]
    description: str = ""


# ---------------------------------------------------------------------------
# 处理函数对“不确定”结果的显式表达
# ---------------------------------------------------------------------------


class StepFailed(Exception):
    """处理函数**明确确认**该步骤业务失败、且没有产生需要撤回的外部效果。

    例如验证样本读数超差：这是确定结论，运行直接转入按序恢复。
    与普通异常不同——普通异常默认表示“不知道控制器那边发生了什么”，
    会先尝试用 op_id 确认，而不是直接判定失败。
    """

    def __init__(self, message: str, detail: Any = None):
        super().__init__(message)
        self.detail = detail


@dataclass(frozen=True)
class Resolution:
    """resolver 对一次结果不明的操作给出的结论基类。"""


@dataclass(frozen=True)
class Committed(Resolution):
    """操作确实已生效。result 为适配器确认的最终结果。"""

    result: Any = None


@dataclass(frozen=True)
class NotExecuted(Resolution):
    """操作确实没有生效（可以安全地按失败处理）。"""


@dataclass(frozen=True)
class Unknown(Resolution):
    """仍然无法确认。运行停在 blocked，等待适配器恢复后继续。"""

    reason: str = ""


@dataclass(frozen=True)
class AmbiguousOutcome:
    """处理函数可显式返回：我发起了操作，但拿不到确定结论。

    库随后调用 resolver（若有），否则用同一 op_id 重试处理函数。
    """

    reason: str = ""
    detail: Any = None


# ---------------------------------------------------------------------------
# 注册表：引用名 -> 可调用对象，流程按 名称+版本 注册
# ---------------------------------------------------------------------------


@dataclass
class Registry:
    """处理函数与流程定义的注册表。

    注册表属于进程内对象（不持久化）；持久化的是每次运行开始时的
    流程快照（引用名、参数、顺序）。服务升级后注册表变化，旧运行
    仍按快照中的引用名解析，缺失即明确报告无法继续。
    """

    _handlers: dict[str, HandlerFn | CompensationFn | ResolverFn] = field(
        default_factory=dict
    )
    _flows: dict[tuple[str, int], Flow] = field(default_factory=dict)

    def handler(
        self, ref: str, fn: HandlerFn | CompensationFn | ResolverFn | None = None
    ):
        """登记处理/恢复/解析函数。

        可作为普通调用 ``reg.handler("apply", fn)``，也可作为装饰器
        ``@reg.handler("apply")`` 使用。
        """

        def _register(f):
            if not callable(f):
                raise FlowValidationError(f"引用 {ref!r} 必须是可调用对象")
            self._handlers[ref] = f
            return f

        return _register if fn is None else _register(fn)

    def get_handler(self, ref: str):
        return self._handlers.get(ref)

    def flow(self, flow: Flow) -> Flow:
        """登记一个流程，并做静态自洽性校验。"""

        if not flow.steps:
            raise FlowValidationError(f"流程 {flow.name} 至少需要一个步骤")
        names: set[str] = set()
        for s in flow.steps:
            if s.name in names:
                raise FlowValidationError(
                    f"流程 {flow.name} 中步骤名重复: {s.name}"
                )
            names.add(s.name)
        key = (flow.name, flow.version)
        if key in self._flows:
            raise FlowValidationError(f"流程 {key} 已注册")
        self._flows[key] = flow
        return flow

    def define(
        self,
        name: str,
        version: int,
        steps: list[Step] | tuple[Step, ...],
        description: str = "",
    ) -> Flow:
        """便捷构造并登记流程。"""

        return self.flow(Flow(name=name, version=version, steps=tuple(steps),
                              description=description))

    def get_flow(self, name: str, version: int | None = None) -> Flow:
        from .errors import FlowNotFoundError

        if version is not None:
            flow = self._flows.get((name, version))
            if flow is None:
                raise FlowNotFoundError(f"流程 {name} 版本 {version} 未注册")
            return flow
        candidates = [f for (n, _v), f in self._flows.items() if n == name]
        if not candidates:
            raise FlowNotFoundError(f"流程 {name} 未注册")
        return max(candidates, key=lambda f: f.version)

    def missing_refs(self, flow: Flow) -> list[str]:
        """返回流程快照引用、但当前注册表无法提供的函数引用名。"""

        refs: list[str] = []
        for s in flow.steps:
            refs.append(s.handler_ref)
            if s.compensation_ref:
                refs.append(s.compensation_ref)
            if s.resolver_ref:
                refs.append(s.resolver_ref)
        missing = [r for r in refs if self._handlers.get(r) is None]
        # 保序去重
        return list(dict.fromkeys(missing))
