"""校准流程示例：处理函数 / 恢复函数 / 流程登记。

三个有序步骤::

    1. read_original  读取原校准参数（结果供 write 步骤和最后的恢复使用）
    2. write_parameters 把新参数写入控制器；恢复函数 restore_original 把仪器
       恢复到第 1 步**已确认读取**到的原参数（不是重启后去读仪器）
    3. validate_sample 读取验证样本；``passed == False`` 时触发整体撤回

注意每个处理函数都只通过 ``ctx.op_id`` 与控制器通信，不自己生成操作身份；
恢复函数需要的全部内容来自 ``result``（write 步骤的已确认结果）和
``ctx.results['read_original']``，这两份数据在执行时已经落进执行记录。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from durable_calibration import (
    CalibrationLibrary,
    Flow,
    FlowStep,
    OperationRejected,
    compensate_when_field_equals,
)
from durable_calibration.engine import HandlerContext

from .fake_controller import (
    ControllerError,
    FakeInstrumentController,
)

FLOW_NAME = "instrument-calibration"
FLOW_VERSION = 1


def build_controller(base_dir: str, scenario_path: str = None) -> FakeInstrumentController:
    """控制器持久存储与执行库刻意分开放置。"""

    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    return FakeInstrumentController(
        str(base / "controller_state.db"),
        scenario_path=str(scenario_path) if scenario_path else None,
    )


# ---------------------------------------------------------------------------
# 处理函数（可跨进程按名字重新解析，因此都是模块级具名函数）
# ---------------------------------------------------------------------------

async def read_original(ctx: HandlerContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """步骤 1：取得原来的校准参数。"""

    controller = build_controller(params["state_dir"], params.get("scenario_path"))
    receipt = await controller.read_original(ctx.op_id)
    return {
        "op_id": ctx.op_id,
        "receipt": receipt,
        "params": receipt["params"],
    }


async def write_parameters(ctx: HandlerContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """步骤 2：把新参数写入控制器。"""

    controller = build_controller(params["state_dir"], params.get("scenario_path"))
    new_params = params["new_params"]
    try:
        receipt = await controller.write_parameters(ctx.op_id, new_params)
    except ControllerError as exc:
        # 明确拒绝：确定没有外部效果。
        raise OperationRejected(str(exc)) from exc
    return {
        "op_id": ctx.op_id,
        "receipt": receipt,
        "applied_params": new_params,
        "original_params": ctx.results["read_original"]["params"],
        "replayed": bool(receipt.get("replayed")),
    }


async def restore_original(ctx: HandlerContext, params: Dict[str, Any],
                           result: Dict[str, Any]) -> Dict[str, Any]:
    """步骤 2 的恢复函数：把仪器恢复到写新参数之前的原参数。

    ``result`` 是 write_parameters 已经确认的执行结果。恢复所需内容来自
    其中固化的 ``original_params``——它源自 read_original 的确认结果，
    即使重启后仪器已经是新参数也不用再去读仪器。
    """

    controller = build_controller(params["state_dir"], params.get("scenario_path"))
    original_params = result["original_params"]
    try:
        receipt = await controller.restore_parameters(ctx.op_id, original_params)
    except ControllerError as exc:
        raise OperationRejected(str(exc)) from exc
    return {
        "op_id": ctx.op_id,
        "receipt": receipt,
        "restored_to": original_params,
        "replayed": bool(receipt.get("replayed")),
    }


async def validate_sample(ctx: HandlerContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """步骤 3：读取验证样本（旁路核对控制器当前参数是否等于目标参数）。"""

    controller = build_controller(params["state_dir"], params.get("scenario_path"))
    expected = params["new_params"]
    receipt = await controller.validate(ctx.op_id, expected)
    return {
        "op_id": ctx.op_id,
        "receipt": receipt,
        "passed": receipt["passed"],
        "current_params": receipt["current_params"],
        "expected_params": receipt["expected_params"],
    }


# ---------------------------------------------------------------------------
# 流程登记
# ---------------------------------------------------------------------------

def build_flow(*, new_params: Dict[str, Any]) -> Flow:
    """登记流程。

    步骤参数全部用 ``$from_run_input`` 占位符声明：流程定义本身可复用，
    每次运行开始时再把本次的 state_dir / scenario_path / new_params 解析成
    纯 JSON 固化进该运行的定义快照。
    """

    state_dir = {"$from_run_input": "state_dir"}
    scenario_path = {"$from_run_input": "scenario_path"}
    return Flow.define(
        FLOW_NAME,
        version=FLOW_VERSION,
        title="仪器校准（取原参数 → 写新参数 → 验证；失败逆序撤回）",
        steps=[
            FlowStep(
                key="read_original",
                title="取得原校准参数",
                handler=read_original,
                params={"state_dir": state_dir, "scenario_path": scenario_path},
            ),
            FlowStep(
                key="write_parameters",
                title="写入新校准参数",
                handler=write_parameters,
                params={
                    "state_dir": state_dir,
                    "scenario_path": scenario_path,
                    "new_params": {"$from_run_input": "new_params"},
                },
                compensator=restore_original,
            ),
            FlowStep(
                key="validate_sample",
                title="读取验证样本",
                handler=validate_sample,
                params={
                    "state_dir": state_dir,
                    "scenario_path": scenario_path,
                    "new_params": {"$from_run_input": "new_params"},
                },
                # 验证不通过 -> 撤回（write 步骤逆序恢复；本步骤无外部效果）。
                compensate_when=compensate_when_field_equals("passed", False),
            ),
        ],
    )


def build_library(runs_db_path: str) -> CalibrationLibrary:
    """创建一个登记好处理函数的库实例（重启后用同样方式重建即可）。"""

    lib = CalibrationLibrary(runs_db_path)
    lib.register_handlers(
        read_original=read_original,
        write_parameters=write_parameters,
        validate_sample=validate_sample,
        restore_original=restore_original,
    )
    return lib


def run_input(state_dir: str, new_params: Dict[str, Any],
              scenario_path: str = "") -> Dict[str, Any]:
    """一次运行的入参（全部可 JSON 化）。"""

    return {
        "state_dir": state_dir,
        "scenario_path": scenario_path or "",
        "new_params": new_params,
    }
