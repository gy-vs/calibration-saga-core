"""场景 3：取消是“持久化的意图 + 把外部效果撤回”，不是取消协程。

时间线
------

1. 创建一个目标增益 5 的运行但不驱动；
2. 在它尚未开始时登记取消意图并 resume：运行不应执行任何校准步骤，
   直接 rolled_back（标记为 cancelled 终态）；
3. 再演示“已经 apply 之后才取消”：apply 已生效，取消必须把增益
   恢复回原值，最终 cancelled，仪器增益 1.0。

取消 Python 协程本身（CancelledError）不等于控制器回到原参数——
本场景用的是 request_cancel / cancel 的持久化意图约定。
"""

from __future__ import annotations

import asyncio

from demo_support import (
    FLOW_NAME,
    controller,
    demo_store,
    engine,
    fresh_workdir,
    print_run,
    use_scenario,
)

use_scenario("cancel")


async def main() -> None:
    fresh_workdir()

    # -- (1) 还没开始就取消：任何校准步骤都不能执行 ----------------------
    ctl = controller()
    eng = engine(ctl)
    rid = "cancel-before-start"
    await eng.start(FLOW_NAME, params={"new_gain": 5.0}, run_id=rid,
                    drive=False)
    await eng.request_cancel(rid, reason="操作员取消")
    view = await eng.resume(rid)
    print_run(view, ctl, title="场景3(1) 开始前取消")
    assert view.state == "cancelled", view.state
    assert all(s.state in ("pending", "skipped") or s.state == "skipped"
               for s in view.steps)
    assert all(s.state == "skipped" for s in view.steps)
    assert ctl.current_gain() == 1.0
    assert ctl.ledger() == []
    print("断言通过：没有任何控制器操作发生。")

    # -- (2) apply 已生效之后取消：必须真正恢复原参数 --------------------
    ctl2 = controller()

    # verify 会等待门闩，使我们能在 apply 已确认、验证进行期间登记取消。
    from examples.calibration.flow import build_registry
    from durable_saga import Engine, Step

    gate = asyncio.Event()

    async def verify_wait_cancel(ctx, params, results):
        try:
            await asyncio.wait_for(gate.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        return {"signal": 50.0, "final_gain": 5.0}

    reg = build_registry(ctl2)
    reg.handler("cal.verify.wait", verify_wait_cancel)
    reg.define(
        FLOW_NAME, 2,
        [
            Step("capture", "cal.capture"),
            Step("apply", "cal.apply",
                 compensation_ref="cal.apply.restore",
                 resolver_ref="cal.apply.resolve"),
            Step("verify", "cal.verify.wait"),
        ],
    )
    eng2 = Engine(demo_store(), reg, lease_ttl_seconds=2.0)
    rid2 = "cancel-after-apply"
    await eng2.start(FLOW_NAME, version=2, params={"new_gain": 5.0},
                     run_id=rid2, drive=False)

    async def driver():
        return await eng2.resume(rid2)

    task = asyncio.create_task(driver())
    # 等待 apply 被确认，verify 挂起，然后取消。
    for _ in range(200):
        v = await eng2.open(rid2, events=False)
        if v.step("apply") and v.step("apply").state == "succeeded":
            break
        await asyncio.sleep(0.01)
    await eng2.request_cancel(rid2, reason="采集中途取消")
    gate.set()
    view2 = await task

    print_run(view2, ctl2, title="场景3(2) apply 生效后取消")
    assert view2.state == "cancelled", view2.state
    assert view2.step("apply").state == "succeeded"
    assert view2.step("verify").state in ("skipped", "succeeded")
    assert ctl2.current_gain() == 1.0, ctl2.current_gain()
    types = [e["op_type"] for e in ctl2.ledger()]
    assert types == ["apply", "restore"], types
    print("\n断言通过：apply 已生效，取消把仪器真正恢复到 1.0。")


if __name__ == "__main__":
    asyncio.run(main())
