"""场景 5：流程随服务升级变化，旧运行仍对应它开始时的步骤含义。

时间线
------

1. 用旧版本注册表（v1，含 cal.apply / cal.verify 等引用）创建并
   只持久化一个运行，不驱动；
2. “升级服务”：新进程的注册表里，apply 引用换成了
   ``cal.apply.v2``（同名步骤但函数是新语义），旧引用不存在；
3. resume 旧运行：库依据**运行快照里固化的引用名**解析，发现缺失，
   明确置为 incompatible 并抛 IncompatibleRunError，绝不换成
   新函数接着跑；
4. 重新部署含旧引用的兼容版本后，resume 正常完成。
"""

from __future__ import annotations

import asyncio

from demo_support import (
    FLOW_NAME,
    controller,
    demo_store,
    fresh_workdir,
    print_run,
    use_scenario,
)
from durable_saga import (
    Engine,
    IncompatibleRunError,
    Registry,
    Step,
)

use_scenario("versioning")


def v1_registry(ctl) -> Registry:
    """旧版本：直接复用示例里完整的 v1 注册。"""

    from examples.calibration.flow import build_registry

    return build_registry(ctl)


def upgraded_registry(ctl) -> Registry:
    """升级后：apply 使用全新的引用名（语义已变），旧引用被移除。"""

    from examples.calibration.flow import build_registry

    reg = build_registry(ctl)  # 其中已注册 cal.apply 等旧引用
    # 模拟旧引用被移除、新引用登场：构造一个只含新引用集的注册表。
    new = Registry()
    new.handler("cal.capture", reg.get_handler("cal.capture"))
    new.handler("cal.apply.v2", reg.get_handler("cal.apply"))
    new.handler("cal.apply.v2.resolve", reg.get_handler("cal.apply.resolve"))
    new.handler("cal.apply.v2.restore", reg.get_handler("cal.apply.restore"))
    new.handler("cal.verify.v2", reg.get_handler("cal.verify"))
    new.define(
        FLOW_NAME, 2,
        [
            Step("capture", "cal.capture"),
            Step("apply", "cal.apply.v2",
                 compensation_ref="cal.apply.v2.restore",
                 resolver_ref="cal.apply.v2.resolve"),
            Step("verify", "cal.verify.v2"),
        ],
    )
    return new


async def main() -> None:
    fresh_workdir()
    rid = "version-demo"

    # 1) 旧版本创建运行
    old_eng = Engine(demo_store(), v1_registry(controller()),
                    lease_ttl_seconds=1.0)
    await old_eng.start(FLOW_NAME, version=1, params={"new_gain": 5.0},
                        run_id=rid, drive=False)
    print(f"[1] 旧服务创建运行 {rid}（快照固化 v1 引用名）")

    # 2) 升级后的新进程
    up_eng = Engine(demo_store(), upgraded_registry(controller()),
                    lease_ttl_seconds=1.0)

    # 新流程本身可以正常启动并完成（用的是 v2 引用）：
    h = await up_eng.start(FLOW_NAME, version=2, params={"new_gain": 5.0},
                           run_id="version-demo-new")
    assert (await up_eng.open(h.run_id)).state == "completed"
    print("[2] 升级后的新流程 v2 可正常完成。")

    # 3) resume 旧运行：缺引用，明确 incompatible
    try:
        await up_eng.resume(rid)
        raise AssertionError("旧运行不应在缺少处理函数时继续")
    except IncompatibleRunError as e:
        print(f"[3] 旧运行被明确拒绝: {e}")
        assert "cal.apply" in e.missing
    view = await up_eng.open(rid)
    print_run(view, controller(), title="场景5(3) 缺少处理函数的旧运行")
    assert view.state == "incompatible"
    assert view.failure["missing_refs"]
    print("missing_refs:", view.failure["missing_refs"])

    # 4) 重新部署包含旧引用的兼容版本：旧运行可以继续到完成
    compat_eng = Engine(demo_store(), v1_registry(controller()),
                        lease_ttl_seconds=1.0)
    view2 = await compat_eng.resume(rid)
    assert view2.state == "completed", view2.state
    print_run(view2, controller(), title="场景5(4) 兼容版本部署后旧运行完成")
    assert view2.result["verify"]["final_gain"] == 5.0
    print("\n断言通过：旧运行按开始时的语义运行，缺函数时明确报错。")


if __name__ == "__main__":
    asyncio.run(main())
