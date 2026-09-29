#!/usr/bin/env python3
"""端到端可复现演示（会真正 SIGKILL 子进程）。

用法::

    python examples/demo.py

它依次演三个场景，每个场景使用独立目录，互不影响：

    1. 验证成功       —— 重开运行看到仪器最终参数；
    2. 写入后进程被杀 —— 记录只到 started，仪器已是新参数；
                         新进程用同一 op_id 查到原回执，继续到成功；
    3. 验证失败       —— 已生效操作按依赖逆序撤回，仪器回到原参数；
                         恢复动作先被拒绝再继续也一并演示。

每一步都会打印执行库的执行事实与仪器的真实参数，便于独立核对。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "examples" / "worker.py"
ENV = dict(os.environ, PYTHONPATH=f"{ROOT / 'src'}:{ROOT}", DCAL_TTL="2")

ORIGINAL = {"gain": 1.0, "offset": 0.0}
NEW = {"gain": 2.5, "offset": 0.125}


def worker(base, cmd, run_id=None, timeout=30):
    args = [sys.executable, str(WORKER), cmd, str(base)]
    if run_id:
        args.append(run_id)
    proc = subprocess.run(args, capture_output=True, text=True,
                          timeout=timeout, env=ENV)
    return proc


def scenario(base, **flags):
    Path(base, "scenario.json").write_text(json.dumps(flags))


def heading(text):
    print("\n" + "=" * 72)
    print(text)
    print("=" * 72)


def show(base, run_id, title):
    print(f"\n--- {title} ---")
    proc = worker(base, "inspect", run_id)
    if proc.returncode != 0:
        print(proc.stderr)
        return
    r = json.loads(proc.stdout)
    print(f"运行状态: {r['status']}")
    print(f"仪器实际参数: {r['instrument_params_now']}")
    for s in r["steps"]:
        line = (f"  步骤 {s['key']:<18} 执行={s['execute_status']:<12}"
                f" 撤回={s['compensate_status']:<12}"
                f" op={s['execute_op_id']}")
        print(line)
    print("执行事实顺序:")
    for f in r["facts"]:
        extra = ""
        if f["detail"]:
            extra = " " + json.dumps(f["detail"], ensure_ascii=False)
        print(f"  #{f['seq']:>3} {f['kind']:<24}{extra}")


def scenario_success(base):
    heading("场景 1：正常校准成功（重启后结果稳定，不重写、不撤回）")
    rid = "success"
    scenario(base)
    p = worker(base, "start", rid)
    assert p.returncode == 0, p.stderr
    show(base, rid, "第一次驱动完成")
    p = worker(base, "resume", rid)
    print(f"\n采集服务重启后再次 resume -> {json.loads(p.stdout)['status']}"
          "（终态幂等）")


def scenario_kill(base):
    heading("场景 2：控制器已接受新参数，进程在记录成功前被 SIGKILL")
    rid = "killed"
    scenario(base, kill_after_write=True,
             kill_barrier=str(Path(base) / "barrier.json"))
    p = worker(base, "start", rid)
    print(f"worker 退出码={p.returncode}（137/-9 表示被 SIGKILL）")
    show(base, rid, "崩溃后的执行记录：只表明 write 曾经开始")

    print("\n关键点：仪器实际已经是新参数，但库没有把它当成失败重发，"
          "也没有当成成功发布结果。")
    print("等租约过期，清除故障，用【全新进程】resume ...")
    scenario(base)  # 控制器/主机恢复
    time.sleep(2.2)
    p = worker(base, "resume", rid)
    print("resume ->", p.stdout.strip(), p.stderr.strip()[-200:])
    show(base, rid, "恢复后：用同一 op_id 查到原回执，继续验证到成功")


def scenario_validation_fail(base):
    heading("场景 3：验证失败 -> 逆序撤回 -> 仪器回到原参数")
    rid = "valfail"
    scenario(base, force_validation_fail=True, restore_reject_times=1)
    p = worker(base, "start", rid)
    assert p.returncode == 0, p.stderr
    show(base, rid, "恢复动作首次被明确拒绝，随后用新 op_id 继续并成功")


def main():
    root = Path(tempfile.mkdtemp(prefix="dcal-demo-"))
    try:
        bases = {name: root / name for name in
                 ("success", "killed", "valfail")}
        for b in bases.values():
            (b / "controller").mkdir(parents=True)

        scenario_success(bases["success"])
        scenario_kill(bases["killed"])
        scenario_validation_fail(bases["valfail"])

        print("\n" + "=" * 72)
        print("全部演示完成。三份独立记录保留在：", root)
        print("=" * 72)
        print("你可以随时再运行（用相同的临时目录需自行替换）：")
        print(f"  python examples/worker.py inspect {bases['killed']} killed")
    except Exception:
        raise


if __name__ == "__main__":
    main()
