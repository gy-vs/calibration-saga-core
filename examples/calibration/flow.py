"""校准流程定义：把仪器控制器接到 durable_saga 的处理/恢复/解析函数。

步骤（有序）
============

1. ``capture`` 读取并保存原校准参数（恢复所需内容来自这里的
   **已确认结果**，重启后绝不会再去读已经改变的仪器状态）；
2. ``apply``   以 op_id 为身份写入新参数；
3. ``verify``  读取验证样本，信号超差则抛 StepFailed——明确失败，
   触发对 apply 的按序恢复。

apply 还注册了 resolver：当处理进程在记录成功前被杀，库重启后
调用它，用同一个 op_id 在控制器台账里核对操作到底有没有生效。
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

# 允许直接以脚本方式运行本目录文件
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from durable_saga import (  # noqa: E402
    AmbiguousOutcome,
    Committed,
    NotExecuted,
    Registry,
    Step,
    StepContext,
    StepFailed,
)
from examples.calibration.controller import (  # noqa: E402
    ControllerUnavailable,
    InstrumentController,
)

FLOW_NAME = "instrument_calibration"
FLOW_VERSION = 1


def build_registry(controller: InstrumentController) -> Registry:
    reg = Registry()

    # -- 1) 取原参数 -----------------------------------------------------

    async def capture(ctx: StepContext, params: Any, results: dict) -> dict:
        # 纯读取；结果里固化原增益，恢复阶段只引用它。
        sample = await asyncio.to_thread(controller.read_sample)
        return {"original_gain": sample["gain"]}

    # -- 2) 写入新参数 ---------------------------------------------------

    async def apply(ctx: StepContext, params: Any, results: dict) -> Any:
        new_gain = float(ctx.run_params["new_gain"])
        try:
            receipt = await asyncio.to_thread(controller.apply, ctx.op_id, new_gain)
        except ControllerUnavailable as e:
            # 明确无法确认控制器是否写入：交给 resolver 约定处理，
            # 而不是当成功或当全新失败。
            return AmbiguousOutcome(reason=f"控制器不可用: {e}")
        return {"applied_gain": receipt["new_gain"],
                "previous_gain": receipt["old_gain"],
                "op_id": ctx.op_id}

    async def resolve_apply(
        ctx: StepContext, params: Any, results: dict
    ):
        """用控制器台账核对同一次操作。"""

        record = await asyncio.to_thread(controller.lookup, ctx.op_id)
        if record is None:
            # 台账里没有：这次操作确实没被接受。
            return NotExecuted()
        if record["status"] != "accepted":
            return NotExecuted()
        receipt = record["receipt"]
        return Committed(result={
            "applied_gain": receipt["new_gain"],
            "previous_gain": receipt["old_gain"],
            "op_id": ctx.op_id,
            "confirmed_by": "ledger_resolver",
        })

    async def restore_apply(
        ctx: StepContext, comp_params: Any, step_result: dict
    ) -> dict:
        # 恢复目标只来自 apply 已确认的回执（previous_gain），
        # 绝不重新读取仪器当前状态。
        target = float(step_result["previous_gain"])
        try:
            receipt = await asyncio.to_thread(
                controller.restore, ctx.op_id, target
            )
        except ControllerUnavailable as e:
            # 恢复动作自身失败/不明：保持未完成，适配器恢复后继续。
            return AmbiguousOutcome(reason=f"控制器不可用，恢复未确认: {e}")
        return {"restored_to": receipt["restored_to"], "op_id": ctx.op_id}

    # -- 3) 读取验证样本 -------------------------------------------------

    async def verify(ctx: StepContext, params: Any, results: dict) -> dict:
        sample = await asyncio.to_thread(controller.read_sample)
        if not sample["valid"]:
            raise StepFailed(
                "验证样本超差",
                {"signal": sample["signal"], "limits": [0, 100],
                 "gain": sample["gain"]},
            )
        return {"signal": sample["signal"], "final_gain": sample["gain"]}

    reg.handler("cal.capture", capture)
    reg.handler("cal.apply", apply)
    reg.handler("cal.apply.resolve", resolve_apply)
    reg.handler("cal.apply.restore", restore_apply)
    reg.handler("cal.verify", verify)

    reg.define(
        FLOW_NAME,
        FLOW_VERSION,
        [
            Step("capture", "cal.capture"),
            Step(
                "apply",
                "cal.apply",
                compensation_ref="cal.apply.restore",
                resolver_ref="cal.apply.resolve",
            ),
            Step("verify", "cal.verify"),
        ],
        description="取原参数→写新参数→读验证样本；失败时恢复原参数",
    )
    return reg
