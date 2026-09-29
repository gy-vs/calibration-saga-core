"""演示场景共享辅助：统一的工作目录、引擎装配与状态打印。"""

from __future__ import annotations

import json
import os
import sys

# 允许从仓库根目录直接运行脚本
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from durable_saga import Engine, Store  # noqa: E402
from examples.calibration.controller import InstrumentController  # noqa: E402
from examples.calibration.flow import (  # noqa: E402
    FLOW_NAME,
    build_registry,
)

WORKDIR = os.path.join(ROOT, "workdir_demo")
RUNS_DB = os.path.join(WORKDIR, "runs.db")
CONTROLLER_DB = os.path.join(WORKDIR, "controller.db")


def demo_store() -> Store:
    """示例专用执行库（DELETE 日志）。场景里直接构造 Engine 时使用。"""

    return Store(RUNS_DB, journal_mode="DELETE")


def use_scenario(name: str) -> None:
    """切换到指定场景独立的工作目录（互不污染，可反复运行）。"""

    global WORKDIR, RUNS_DB, CONTROLLER_DB
    WORKDIR = os.path.join(ROOT, "workdir_demo", name)
    RUNS_DB = os.path.join(WORKDIR, "runs.db")
    CONTROLLER_DB = os.path.join(WORKDIR, "controller.db")


def fresh_workdir() -> None:
    os.makedirs(WORKDIR, exist_ok=True)
    for f in os.listdir(WORKDIR):
        path = os.path.join(WORKDIR, f)
        try:
            os.remove(path)
        except OSError:
            # Windows 或仍有连接时可能删不掉；忽略，打开时会被覆盖。
            pass


def controller() -> InstrumentController:
    return InstrumentController(CONTROLLER_DB, default_gain=1.0)


def engine(controller: InstrumentController | None = None,
           ttl: float = 2.0) -> Engine:
    controller = controller or InstrumentController(CONTROLLER_DB)
    return Engine(
        demo_store(),
        build_registry(controller),
        lease_ttl_seconds=ttl,
    )


def print_run(view, controller: InstrumentController | None = None,
              title: str = "") -> None:
    """把一次运行的可核对事实完整打印出来。"""

    if title:
        print(f"\n===== {title} =====")
    print(f"run_id            : {view.run_id}")
    print(f"state             : {view.state}"
          + (f" (blocked: {view.blocked_reason})" if view.is_blocked else ""))
    print(f"cancel_requested  : {view.cancel_requested}")
    print("steps:")
    for s in view.steps:
        line = f"  [{s.seq}] {s.name:<8} {s.state:<9}"
        if s.op_id:
            line += f" op_id={s.op_id}"
        if s.state == "succeeded":
            line += f" result={json.dumps(s.result, ensure_ascii=False)}"
        if s.state in ("failed", "unknown"):
            line += f" error={json.dumps(s.error, ensure_ascii=False)}"
        print(line)
    if view.compensations:
        print("compensations (依赖逆序):")
        for c in view.compensations:
            line = f"  {c.step_name:<8} {c.state:<12}"
            if c.op_id:
                line += f" op_id={c.op_id}"
            if c.state == "succeeded":
                line += f" result={json.dumps(c.result, ensure_ascii=False)}"
            if c.state == "unknown":
                line += f" error={json.dumps(c.error, ensure_ascii=False)}"
            print(line)
    if view.terminal and view.result is not None:
        print(f"final result      : {json.dumps(view.result, ensure_ascii=False)}")
    if view.failure is not None:
        print(f"failure           : {json.dumps(view.failure, ensure_ascii=False)}")
    if controller is not None:
        print(f"controller gain   : {controller.current_gain()}")
    print("facts (只追加事件):")
    for fct in view.facts():
        print(f"  #{fct['seq']:>2} {fct['at'][11:23]} {fct['kind']:<22}"
              f" {fct['step'] or '':<8}")
