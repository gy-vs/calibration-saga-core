"""库对外抛出的异常类型。"""

from __future__ import annotations


class DurableSagaError(Exception):
    """所有库异常的基类。"""


class RunNotFoundError(DurableSagaError):
    """指定的运行标识不存在。"""


class FlowNotFoundError(DurableSagaError):
    """注册表中找不到指定名称/版本的流程。"""


class FlowValidationError(DurableSagaError):
    """流程定义本身不合法（步骤重名、缺少处理函数引用等）。"""


class RunBusyError(DurableSagaError):
    """运行正被一个未过期的租约驱动，当前调用不能取得进展权。

    这不是失败：运行仍在被另一个持有者（可能仍存活的进程）推进，
    调用方稍后重试，或等待租约过期后接管即可。
    """

    def __init__(self, run_id: str, owner: str, expires_at: str):
        self.run_id = run_id
        self.owner = owner
        self.expires_at = expires_at
        super().__init__(
            f"运行 {run_id} 正被 {owner} 的有效租约占用（至 {expires_at}）"
        )


class IncompatibleRunError(DurableSagaError):
    """过去的运行引用了当前注册表中不存在的处理函数。

    运行被明确置为 ``incompatible`` 终态前，恢复入口也会抛出该异常，
    提示必须由调用方提供与运行开始时语义一致的函数，库不会换用
    同名的新函数继续执行。
    """

    def __init__(self, run_id: str, missing: list[str]):
        self.run_id = run_id
        self.missing = missing
        super().__init__(
            f"运行 {run_id} 缺少处理函数引用: {', '.join(missing)}"
        )
