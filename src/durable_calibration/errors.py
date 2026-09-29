"""库对外暴露的异常类型。

异常分两类语义，恢复逻辑据此区分：

- :class:`OperationRejected` —— 处理函数明确判定外部操作**确定未生效**
  （例如控制器返回"操作被拒绝/参数非法"）。这类失败可以安全地用新操作身份重试。
- 其它所有异常 —— 视为**结果不明**：控制器可能已经执行也可能没有，
  必须用原 ``op_id`` 按幂等约定查询，绝不能直接重发一次新操作。
"""

from __future__ import annotations


class DCError(Exception):
    """库内所有异常的基类。"""


class UnknownRun(DCError):
    """运行标识不存在。"""


class UnknownFlow(DCError):
    """登记流程时引用了不存在的流程，或流程标识未知。"""


class FlowVersionMismatch(DCError):
    """同名流程以不一致的定义或版本重复登记。"""


class InvalidState(DCError):
    """对当前状态不允许的操作（如对已结束的运行请求取消）。"""


class RunLocked(DCError):
    """运行正被其它驱动者（或另一个进程）持有有效租约。

    可稍后重试；只有确认旧进程已死亡且超过租约时间后才能接管。
    """


class RunLockLost(DCError):
    """驱动过程中租约失效（被更新的 owner/fencing 接管），本次进展不再有效。"""


class HandlerUnavailable(DCError):
    """运行引用的步骤处理函数或恢复函数在当前库实例中没有登记。

    旧运行按其开始时的步骤定义恢复；缺少对应函数时明确停止上报，
    而不是换成同名的新函数继续执行。
    """


class LibraryProgrammingError(DCError):
    """调用方对库的用法错误（参数不可 JSON 化、重复 step key 等）。"""


class OperationRejected(Exception):
    """处理函数可抛出此异常表示：外部操作被明确拒绝，确定没有外部效果。

    注意它**不继承** :class:`DCError`：它是处理函数与库之间的控制约定，
    库会把该尝试记为 ``rejected``（确定失败），而不是 ``inconclusive``。
    ``receipt`` 可保存控制器给出的拒绝回执（须可 JSON 化）。
    """

    def __init__(self, message: str, receipt: object = None) -> None:
        super().__init__(message)
        self.receipt = receipt
