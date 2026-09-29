"""持久化校准流程编排库（嵌入式，SQLite + asyncio）。

核心抽象见 README：
- :class:`Flow`      有序的校准步骤定义（开始运行时整体固化为 JSON 快照）；
- :class:`CalibrationLibrary`  登记流程 / 启动运行 / 重开运行 / 取消 / 查询；
- :class:`RunSnapshot`         一次运行及其每一步、补偿、尝试的独立状态视图。
"""

from .errors import (
    DCError,
    FlowVersionMismatch,
    HandlerUnavailable,
    InvalidState,
    LibraryProgrammingError,
    OperationRejected,
    RunLocked,
    RunLockLost,
    UnknownFlow,
    UnknownRun,
)
from .models import (
    AttemptSnapshot,
    Flow,
    FlowStep,
    RunSnapshot,
    StepSnapshot,
    compensate_when_field_equals,
)
from .library import CalibrationLibrary

__all__ = [
    "CalibrationLibrary",
    "Flow",
    "FlowStep",
    "RunSnapshot",
    "StepSnapshot",
    "AttemptSnapshot",
    "compensate_when_field_equals",
    "DCError",
    "InvalidState",
    "UnknownRun",
    "UnknownFlow",
    "FlowVersionMismatch",
    "HandlerUnavailable",
    "OperationRejected",
    "RunLocked",
    "RunLockLost",
    "LibraryProgrammingError",
]

__version__ = "0.1.0"
