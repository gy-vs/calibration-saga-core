"""场景 4：两个采集进程连接同一份执行记录时，只有一套有效进展。

演示两种保护：

1. 租约：一个运行正被活跃驱动时，第二个进程的 resume 得到
   RunBusyError，不会并发推进；
2. fencing epoch：持有过期租约的旧进程（模拟“原进程尚未确认退出”）
   即使在外部调用返回后试图继续写状态，也会被 epoch 拒绝；新持有者
   完成的运行不会被旧持有者覆盖或触发恢复。
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
from durable_saga import Engine, RunBusyError
from durable_saga.store import LeaseLostError

use_scenario("concurrency")


async def main() -> None:
    fresh_workdir()
    ctl = controller()

    # -- (1) 并发 resume：第二个被 RunBusyError 拒绝 ---------------------
    from examples.calibration.flow import build_registry
    from durable_saga import Step

    gate = asyncio.Event()
    reg_a = build_registry(controller())

    async def slow_apply(ctx, params, results):
        # 持有租约期间停在外部调用里，给第二个进程制造并发窗口。
        await asyncio.wait_for(gate.wait(), timeout=3.0)
        return {"applied_gain": 5.0, "previous_gain": 1.0,
                "op_id": ctx.op_id}

    reg_a.handler("slow.apply", slow_apply)
    reg_a.define(
        FLOW_NAME, 2,
        [
            Step("capture", "cal.capture"),
            Step("apply", "slow.apply",
                 compensation_ref="cal.apply.restore",
                 resolver_ref="cal.apply.resolve"),
            Step("verify", "cal.verify"),
        ],
    )
    eng_a = Engine(demo_store(), reg_a, lease_ttl_seconds=30.0)
    rid = "lease-demo"
    await eng_a.start(FLOW_NAME, version=2, params={"new_gain": 5.0},
                      run_id=rid, drive=False)

    driver = asyncio.create_task(eng_a.resume(rid))
    await asyncio.sleep(0.15)  # 让 A 取得租约并进入 slow.apply

    eng_b = engine(controller(), ttl=30.0)
    try:
        await eng_b.resume(rid)
        raise AssertionError("第二个进程不应取得租约")
    except RunBusyError as e:
        print(f"[1] 并发 resume 被拒绝: {e}")

    # 旁观视图仍可随时只读核对。
    peek = await eng_b.open(rid)
    print(f"[1] 旁观：state={peek.state} apply={peek.step('apply').state}")

    gate.set()
    view = await driver
    assert view.state == "completed", view.state
    print("[1] 持有者完成运行，旁观者没有造成任何重复操作。\n")

    # -- (2) fencing：过期租约持有者的迟到写入被拒绝 ---------------------
    # 用一个全新运行：旧引擎用短 TTL 占用，然后我们不驱动它、
    # 等待租约过期；新引擎接管并完成；旧引擎再尝试带旧 epoch 写事件。
    rid2 = "fence-demo"
    eng_old = engine(controller(), ttl=0.3)
    await eng_old.start(FLOW_NAME, params={"new_gain": 5.0}, run_id=rid2,
                        drive=False)
    epoch_old = await asyncio.to_thread(
        eng_old.store.acquire_lease, rid2, eng_old.owner, 0.3
    )
    print(f"[2] 旧持有者 owner={eng_old.owner} epoch={epoch_old}")
    await asyncio.sleep(0.45)  # 租约过期

    eng_new = engine(controller(), ttl=5.0)
    view_new = await eng_new.resume(rid2)
    assert view_new.state == "completed"
    epoch_now = view_new.lease_epoch
    print(f"[2] 新持有者接管并完成；当前 epoch={epoch_now}（旧 epoch 失效）")

    # 旧持有者迟到的、带旧 epoch 的写入必须失败：
    refused = False
    try:
        await asyncio.to_thread(
            eng_old.store.append_event, rid2, eng_old.owner, epoch_old,
            "stale_write_from_old_owner",
        )
    except LeaseLostError:
        refused = True
    assert refused, "过期 epoch 的写入必须被拒绝"
    print("[2] 旧持有者的迟到写入被 fencing 拒绝（LeaseLostError）。")

    final = await eng_old.open(rid2)
    assert final.state == "completed"
    kinds = [fct["kind"] for fct in final.facts()]
    assert "stale_write_from_old_owner" not in kinds
    print_run(final, controller(), title="场景4 最终状态（未被旧持有者污染）")
    print("\n断言通过：同一运行在两个进程间只有一套有效进展。")


if __name__ == "__main__":
    asyncio.run(main())
