"""场景 1：进程在“控制器已接受新参数、执行记录尚未确认”之间被杀。

时间线
------

进程 A：
  1. 创建运行，目标增益 5（信号 50，合法，最终会验证成功）；
  2. 在控制器上武装“apply 提交后立即 os._exit”；
  3. 驱动运行，进程以退出码 42 消失。

随后（模拟重启后的新进程 B）：
  4. 重开同一运行：apply 步骤停在 started，仪器实际增益已经是 5；
  5. resume —— 库用同一 op_id 查询控制器台账，拿到最初回执，
     确认 apply 已生效，继续验证并完成；
  6. 打印最终参数与全部执行事实。

关键点：不会产生第二次 apply，也不会把 started 当成失败重来。

用法
----

    python3 scenario_kill_recover.py              # 完整演示（A+B）
    python3 scenario_kill_recover.py --drive-killed --workdir=<dir>
                                                  # 仅扮演被杀的进程 A
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys

import demo_support as ds
from durable_saga import RunBusyError

# 先确定场景工作目录，再创建任何引擎/控制器。
if "--drive-killed" in sys.argv:
    _wd = next(a.split("=", 1)[1]
               for a in sys.argv if a.startswith("--workdir="))
    ds.WORKDIR = _wd
    ds.RUNS_DB = os.path.join(_wd, "runs.db")
    ds.CONTROLLER_DB = os.path.join(_wd, "controller.db")
else:
    ds.use_scenario("kill_recover")

KILL_RUN_ID = "kill-demo-run"
TARGET_GAIN = 5.0


async def phase_a() -> None:
    ds.fresh_workdir()
    ctl = ds.controller()
    eng = ds.engine(ctl)
    await eng.start(
        ds.FLOW_NAME,
        params={"new_gain": TARGET_GAIN},
        run_id=KILL_RUN_ID,
        drive=False,
    )
    print(f"[A] 运行已创建；控制器初始增益 {ctl.current_gain()}")
    print(f"[A] 工作目录 {ds.WORKDIR}")
    # 自杀标记由子进程自己武装（见 drive_killed），避免父进程在
    # 随后的 phase B 中误触。
    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__),
         "--drive-killed", f"--workdir={ds.WORKDIR}"],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env={**os.environ, "PYTHONPATH": ds.ROOT},
    )
    rc = proc.wait()
    print(f"[A] 驱动进程退出码 = {rc}（42 表示控制器已接受新参数、"
          f"执行库尚未确认时进程消失）")


async def drive_killed() -> None:
    ctl = ds.controller()
    # 武装后立刻 resume：apply 提交即 os._exit(42)。
    ctl.arm_kill_before_response("apply")
    eng = ds.engine(ctl)
    await eng.resume(KILL_RUN_ID)
    print("不应出现：进程没有在 apply 后被杀", file=sys.stderr)
    sys.exit(1)


async def phase_b() -> None:
    ctl = ds.controller()
    eng = ds.engine(ctl)

    reopened = await eng.open(KILL_RUN_ID)
    apply_step = reopened.step("apply")
    print(f"[B] 重启后重开：apply 状态 = {apply_step.state}，"
          f"op_id = {apply_step.op_id}")
    print(f"[B] 仪器实际增益 = {ctl.current_gain()}"
          f"（控制器台账已接受，执行记录却只表明 started）")

    # 原进程的租约可能还没到期：此时恢复被拒绝（不是失败）。
    try:
        view = await eng.resume(KILL_RUN_ID)
        print("[B] 立即取得租约（原租约已过期）")
    except RunBusyError as e:
        print(f"[B] {e} —— 等待租约过期后接管（不并发推进）")
        await asyncio.sleep(2.2)
        view = await eng.resume(KILL_RUN_ID)

    ds.print_run(view, ctl, title="场景1 结果：验证成功后的最终参数")

    # 断言：控制器台账中 apply 只有一次，不是两次
    applies = [e for e in ctl.ledger() if e["op_type"] == "apply"]
    assert len(applies) == 1, f"apply 被重复执行: {applies}"
    assert view.state == "completed"
    assert view.result["verify"]["final_gain"] == TARGET_GAIN
    assert ctl.current_gain() == TARGET_GAIN
    print("\n断言通过：apply 只有一次（op_id 去重），最终增益 5.0。")


async def main() -> None:
    if "--drive-killed" in sys.argv:
        await drive_killed()
        return
    await phase_a()
    await phase_b()


if __name__ == "__main__":
    asyncio.run(main())
