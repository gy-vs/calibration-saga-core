"""场景 2：验证失败触发按序恢复；恢复动作自身失败后继续本次运行。

时间线
------

1. 目标增益 20 → 信号 200，超出 [0,100]，验证步骤明确失败
   （StepFailed）；尚未执行的校准不能继续；
2. 故障注入：apply 提交后仪器立即进入（持久）离线——apply 已生效，
   但 restore 无法完成。运行停在 blocked，并明确报告仪器尚未恢复
   到什么状态；
3. 适配器恢复（重新上线）后 resume 同一运行：restore 用同一 op_id
   完成，最终仪器回到原增益 1.0，运行 rolled_back。

同时演示：再次 resume 一个终态运行不会重复写入或撤回。
"""

from __future__ import annotations

import asyncio
import json

from demo_support import (
    FLOW_NAME,
    controller,
    engine,
    fresh_workdir,
    print_run,
    use_scenario,
)

use_scenario("rollback")

BAD_GAIN = 20.0  # 信号 = 200，超差


async def main() -> None:
    fresh_workdir()
    ctl = controller()
    ctl.arm_offline_after("apply")  # apply 一成功，仪器立刻离线

    eng = engine(ctl)
    run_id = "rollback-demo-run"
    await eng.start(
        FLOW_NAME,
        params={"new_gain": BAD_GAIN},
        run_id=run_id,
        drive=False,
    )
    view = await eng.resume(run_id)

    print_run(view, ctl, title="场景2(1) 验证失败 + 恢复动作失败")
    assert view.state == "compensating", view.state
    assert view.is_blocked, "恢复失败时运行必须停在 blocked"
    comp = next(c for c in view.compensations if c.step_name == "apply")
    assert comp.state == "unknown", comp.state
    assert ctl.current_gain() == BAD_GAIN, "仪器仍是坏参数"
    print(f"\n仪器当前增益 = {ctl.current_gain()}（尚未恢复）")
    print(f"blocked_reason 明确指出：{view.blocked_reason}")

    # ---- 适配器恢复正常，继续处理同一次运行 ---------------------------
    ctl.set_offline(False)
    print("\n适配器重新上线，resume 同一运行 ...")
    view2 = await eng.resume(run_id)
    print_run(view2, ctl, title="场景2(2) 恢复完成后的原参数")
    assert view2.state == "rolled_back", view2.state
    assert ctl.current_gain() == 1.0, ctl.current_gain()
    restored = next(c for c in view2.compensations if c.step_name == "apply")
    assert restored.state == "succeeded"
    # 恢复目标来自 apply 的已确认回执，不是重新读取仪器：
    assert restored.result["restored_to"] == 1.0

    # 控制器台账：一次 apply + 一次 restore，没有重复
    types = [(e["op_type"], e["receipt"].get("op_id")) for e in ctl.ledger()]
    print(f"\n控制器台账: {json.dumps(types, ensure_ascii=False)}")
    assert [t for t, _ in types] == ["apply", "restore"]

    # ---- 终态幂等：再 resume 不会重复写或撤回 --------------------------
    view3 = await eng.resume(run_id)
    assert view3.state == "rolled_back"
    assert len(ctl.ledger()) == 2
    assert ctl.current_gain() == 1.0
    print("\n断言通过：终态再次 resume 不产生任何新操作。")


if __name__ == "__main__":
    asyncio.run(main())
