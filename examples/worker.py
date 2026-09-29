"""端到端脚本：真正用 SIGKILL 终止进程，验证崩溃后的状态确认与恢复。

子进程工作方式（worker）：
    python worker.py start   <run_id>     # 登记运行并开始驱动（可能被杀）
    python worker.py resume  <run_id>     # 重开原运行（新进程）
    python worker.py inspect <run_id>     # 只读打印执行事实

父进程（demo_runner）：
    1. 用 kill_after_write 场景启动 worker，等其收到 SIGKILL（或观察屏障文件）；
    2. 用全新进程 resume，验证：不重新写参数，而是用同一 op_id 查到最初回执，
       继续验证；
    3. 分别演示"验证成功看最终参数"与"验证失败并完成恢复看原参数"。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

# 允许直接从仓库根目录运行。
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from examples.calibration_flow import (  # noqa: E402
    FLOW_NAME,
    FLOW_VERSION,
    build_controller,
    build_flow,
    build_library,
    run_input,
)

NEW_PARAMS = {"gain": 2.5, "offset": 0.125}


def _ttl() -> float:
    # 演示/测试用短租约，崩溃后只需等 TTL 秒即可被新进程接管。
    return float(os.environ.get("DCAL_TTL", "3.0"))


def _paths(base: str):
    base = Path(base)
    base.mkdir(parents=True, exist_ok=True)
    return {
        "state_dir": str(base / "controller"),
        "runs_db": str(base / "runs.db"),
        "scenario": str(base / "scenario.json"),
        "barrier": str(base / "kill_barrier.json"),
    }


def write_scenario(paths: dict, **flags) -> None:
    Path(paths["scenario"]).write_text(json.dumps(flags), encoding="utf-8")


async def _cmd_start(base: str, run_id: str) -> str:
    p = _paths(base)
    # 清掉可能存在的旧屏障。
    if os.path.exists(p["barrier"]):
        os.remove(p["barrier"])

    lib = build_library(p["runs_db"])
    flow = build_flow(new_params=NEW_PARAMS)
    lib.register_flow(flow)
    rid = await lib.start_run(
        FLOW_NAME,
        flow_version=FLOW_VERSION,
        run_input=run_input(p["state_dir"], NEW_PARAMS, p["scenario"]),
        run_id=run_id,
    )
    # 让心跳/租约足够长，进程死后需要由父进程等待过期再接管。
    try:
        await lib.run(rid, ttl_seconds=_ttl())
    finally:
        await lib.close()
    print(json.dumps({"run_id": rid}))
    return rid


async def _cmd_resume(base: str, run_id: str) -> None:
    p = _paths(base)
    lib = build_library(p["runs_db"])
    # resume 时恢复所需函数都要登记（按运行开始时的名字解析）。
    lib.register_flow(build_flow(new_params=NEW_PARAMS))
    snap = await lib.resume(run_id, ttl_seconds=_ttl(), force=True)
    await lib.close()
    print(json.dumps({"run_id": run_id, "status": snap.status}, ensure_ascii=False))


async def _cmd_cancel(base: str, run_id: str) -> None:
    p = _paths(base)
    lib = build_library(p["runs_db"])
    # 取消只是持久标记；真正的回滚由随后的 resume 按外部效果完成。
    await lib.request_cancel(run_id)
    snap = await lib.resume(run_id, ttl_seconds=_ttl(), force=True)
    await lib.close()
    print(json.dumps({"run_id": run_id, "status": snap.status},
                     ensure_ascii=False))


async def _cmd_inspect(base: str, run_id: str) -> None:
    p = _paths(base)
    lib = build_library(p["runs_db"])
    snap = await lib.get_run(run_id)
    controller = build_controller(p["state_dir"], None)
    report = {
        "run_id": run_id,
        "status": snap.status,
        "blocked_reason": snap.blocked_reason,
        "instrument_params_now": controller.current_params_sync(),
        "steps": [
            {
                "key": s.key,
                "execute_status": s.execute_status,
                "result": s.result,
                "execute_op_id": s.execute_attempt.op_id
                if s.execute_attempt else None,
                "execute_attempt_status": s.execute_attempt.status
                if s.execute_attempt else None,
                "compensate_status": s.compensate_status,
                "compensate_op_id": s.compensate_attempt.op_id
                if s.compensate_attempt else None,
                "compensate_attempt_status": s.compensate_attempt.status
                if s.compensate_attempt else None,
                "compensate_error": s.compensate_error,
                "error": s.error,
            }
            for s in snap.steps
        ],
        "facts": [
            {"seq": f.seq, "kind": f.kind, "step": f.step_key,
             "detail": f.detail}
            for f in snap.facts
        ],
    }
    await lib.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))


def main() -> None:
    cmd = sys.argv[1]
    base = sys.argv[2]
    run_id = sys.argv[3] if len(sys.argv) > 3 else "demo-run"
    if cmd == "start":
        asyncio.run(_cmd_start(base, run_id))
    elif cmd == "resume":
        asyncio.run(_cmd_resume(base, run_id))
    elif cmd == "cancel":
        asyncio.run(_cmd_cancel(base, run_id))
    elif cmd == "inspect":
        asyncio.run(_cmd_inspect(base, run_id))
    else:
        raise SystemExit(f"unknown command {cmd}")


if __name__ == "__main__":
    main()
