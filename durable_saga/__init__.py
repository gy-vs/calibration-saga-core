"""durable_saga —— 进程退出后仍可恢复确认的有序异步步骤库。

典型用途是仪器校准：先取原参数、再写新参数、随后读验证样本；
任何一步验证不通过时，按依赖关系撤回已经生效的外部操作。

库只使用 Python 标准库（sqlite3 + asyncio），执行事实保存在本地
SQLite 中，不依赖 Redis、消息代理或云端工作流服务。
"""

from .contracts import (
    AmbiguousOutcome,
    Committed,
    Flow,
    NotExecuted,
    Registry,
    Resolution,
    Step,
    StepContext,
    StepFailed,
    Unknown,
)
from .engine import Engine, RunHandle
from .errors import (
    DurableSagaError,
    FlowNotFoundError,
    FlowValidationError,
    IncompatibleRunError,
    RunBusyError,
    RunNotFoundError,
)
from .store import Store
from .views import CompensationView, RunView, StepView

__all__ = [
    "AmbiguousOutcome",
    "Committed",
    "NotExecuted",
    "Unknown",
    "Resolution",
    "Step",
    "StepContext",
    "StepFailed",
    "Flow",
    "Registry",
    "Engine",
    "RunHandle",
    "Store",
    "DurableSagaError",
    "FlowNotFoundError",
    "FlowValidationError",
    "IncompatibleRunError",
    "RunBusyError",
    "RunNotFoundError",
    "RunView",
    "StepView",
    "CompensationView",
]
