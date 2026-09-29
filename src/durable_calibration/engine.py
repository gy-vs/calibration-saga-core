"""执行驱动：租约、心跳、外部调用与崩溃后的再确认。

驱动循环的每一圈::

    1. 在 SQLite 短事务里领取下一个动作（可能同时产生一条 started 尝试）；
    2. 若动作是对控制器的调用，在事务外执行（慢操作不持数据库写锁）；
    3. 在另一个带 fencing 的短事务里落结果并推进状态机。

进程被杀时，第 2 步对应的 ``started`` 尝试就是"操作可能已经发生"的持久事实；
重开运行时领到的是**同一个 op_id 的 replay 动作**，由适配器的幂等约定
（同 op_id 重复查询返回最初结果）完成确认，而不是重发一次新操作。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from . import models as m
from .errors import OperationRejected, RunLocked, RunLockLost
from .storage import (
    ACT_BLOCKED,
    ACT_COMPENSATE,
    ACT_CONTINUE,
    ACT_DONE,
    ACT_EXECUTE,
    Database,
    _error_payload,
)


@dataclass
class HandlerContext:
    """传给步骤处理函数 / 恢复函数的上下文。

    处理函数签名::

        async def handle(ctx: HandlerContext, params: dict) -> Any

    恢复函数签名（``result`` 是该步骤已确认的执行结果）::

        async def compensate(ctx: HandlerContext, params: dict, result: Any) -> Any
    """

    run_id: str
    step_key: str
    op_id: str
    kind: str                       # "execute" / "compensate"
    replay: bool                    # True 表示这是用原 op_id 做的再确认
    params: Optional[Dict[str, Any]]
    run_input: Any
    results: Dict[str, Any] = field(default_factory=dict)
    result: Any = None              # 恢复函数对应的已确认执行结果
    fencing_epoch: int = 0

    @property
    def is_compensation(self) -> bool:
        return self.kind == m.ATTEMPT_KIND_COMP


async def drive_run(
    db: Database,
    run_id: str,
    handlers: Dict[str, Any],
    *,
    ttl_seconds: float = 30.0,
    force: bool = False,
    max_replays: int = 3,
) -> m.RunSnapshot:
    """取得租约并把运行向前驱动，直到终态或阻塞，返回最终快照。

    :param max_replays: 同一次未关闭尝试在**本次驱动**中用同一 op_id 再确认
        的最多次数。若再确认仍持续结果不明（例如适配器还没恢复，或处理函数
        确定性地抛错），本次驱动返回阻塞快照而不是忙等；外部调用方稍后可
        再次 :func:`resume`，计数随之清零，开放尝试始终保留。
    :raises RunLocked: 其它驱动者仍持有未过期租约。
    :raises RunLockLost: 驱动过程中租约被接管。
    """

    owner = uuid.uuid4().hex
    # 每次驱动使用独立的工作线程池，drive 结束即关闭。这样即使驱动协程被
    # 取消、进程内还有其它 asyncio 运行时，也不会有心跳线程残留在解释器的
    # 默认执行器上饿死后续 DB 操作。
    executor = ThreadPoolExecutor(max_workers=4,
                                  thread_name_prefix="dcal-drive")

    async def _to_thread(fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            executor, lambda: fn(*args, **kwargs))

    epoch = await _to_thread(
        db.acquire_lease, run_id, owner,
        ttl_seconds=ttl_seconds, force=force,
    )
    if epoch is None:
        executor.shutdown(wait=False, cancel_futures=True)
        raise RunLocked(
            f"运行 {run_id} 正被其它进程/驱动者持有有效租约；"
            "确认旧进程已退出并等待租约过期后再恢复。"
        )

    heartbeat_interval = max(0.1, ttl_seconds / 3)
    hb_stop = asyncio.Event()

    async def _heartbeat() -> None:
        while not hb_stop.is_set():
            try:
                await asyncio.wait_for(hb_stop.wait(), timeout=heartbeat_interval)
                return
            except asyncio.TimeoutError:
                pass
            await _to_thread(db.heartbeat, run_id, owner, epoch, ttl_seconds)

    hb_task = asyncio.create_task(_heartbeat())

    def _resolve(name: str):
        return handlers.get(name)

    # 同一 attempt_id 在本次驱动内的再确认次数；跨 resume 重新计数。
    replay_count: Dict[int, int] = {}

    try:
        while True:
            action = await _to_thread(
                db.plan_next_action, run_id, owner, epoch, _resolve
            )
            kind = action["action"]

            if kind == ACT_DONE:
                return await _to_thread(db.load_snapshot, run_id)
            if kind == ACT_CONTINUE:
                continue
            if kind == ACT_BLOCKED:
                return await _to_thread(db.load_snapshot, run_id)

            if action.get("replay"):
                seen = replay_count.get(action["attempt_id"], 0)
                # seen 是此前已经做过的再确认次数；本次是第 seen+1 次。
                if seen + 1 > max(0, max_replays):
                    # 再确认持续没有明确结果：交还快照，等待适配器恢复后由
                    # 调用方再次 resume。开放尝试与阻塞原因都已在库里。
                    return await _to_thread(db.load_snapshot, run_id)
                replay_count[action["attempt_id"]] = seen + 1

            await _invoke_and_finish(
                db, run_id, owner, epoch, action, handlers, _to_thread
            )
    finally:
        hb_stop.set()
        hb_task.cancel()
        try:
            await hb_task
        except BaseException:  # noqa: BLE001
            pass
        # 归还租约；若已失效（被接管），忽略——这正是 fencing 的预期。
        try:
            await _to_thread(db.release_lease, run_id, owner, epoch)
        except BaseException:  # noqa: BLE001
            pass
        executor.shutdown(wait=False, cancel_futures=True)


async def _invoke_and_finish(db, run_id, owner, epoch, action, handlers,
                             to_thread):
    kind = action["action"]
    handler_name = action["handler"]
    fn = handlers.get(handler_name)
    if fn is None:
        # 领取后处理函数被注销属于编程用法问题；正常情况下 plan 阶段已拦截。
        raise RuntimeError(
            f"步骤 {action['step_key']} 已领取执行但处理函数 {handler_name!r}"
            " 未登记"
        )

    params, predicate = await to_thread(
        db.load_step_invocation, run_id, action["step_seq"]
    )
    run_input, confirmed = await to_thread(
        db.load_invocation_context, run_id
    )

    ctx = HandlerContext(
        run_id=run_id,
        step_key=action["step_key"],
        op_id=action["op_id"],
        kind=m.ATTEMPT_KIND_EXEC if kind == ACT_EXECUTE else m.ATTEMPT_KIND_COMP,
        replay=bool(action.get("replay")),
        params=params,
        run_input=run_input,
        results=confirmed,
        fencing_epoch=epoch,
    )
    if kind == ACT_COMPENSATE:
        ctx.result = confirmed.get(action["step_key"])

    outcome, result, error = await _safe_invoke(fn, ctx, kind)

    await to_thread(
        db.finish_attempt,
        run_id, owner, epoch,
        action=action,
        outcome_status=outcome,
        result=result,
        error=error,
        predicate=predicate,
    )


async def _safe_invoke(fn, ctx, kind):
    """执行一次外部调用并把结果分类为 succeeded / rejected / inconclusive。"""

    try:
        if kind == ACT_COMPENSATE:
            if ctx.step_key not in ctx.results:
                # 防御：恢复所需内容必须来自已确认的执行结果。
                raise RuntimeError(
                    f"步骤 {ctx.step_key!r} 缺少已确认的执行结果，"
                    "不能执行恢复（不允许重启后去读已改变的仪器状态）。"
                )
            value = fn(ctx, ctx.params, ctx.result)
        else:
            value = fn(ctx, ctx.params)
        if inspect.isawaitable(value):
            value = await value

        # 返回值必须能保存为 JSON；不能保存则等同于结果不明。
        try:
            json.dumps(value, default=str)
        except (TypeError, ValueError) as exc:
            return (
                m.ATTEMPT_INCONCLUSIVE,
                None,
                {"type": "ResultNotJSONSerializable",
                 "message": f"返回值不能保存为 JSON：{exc}"},
            )
        return m.ATTEMPT_SUCCEEDED, value, None

    except OperationRejected as exc:
        receipt = exc.receipt
        return m.ATTEMPT_REJECTED, None, {
            "type": type(exc).__name__,
            "message": str(exc),
            "receipt": receipt,
        }
    except asyncio.CancelledError:
        # 协程被取消不代表控制器回滚：started 尝试保留，交由恢复流程用原
        # op_id 确认。这里不做落库，直接向上抛，语义与进程被杀一致。
        raise
    except Exception as exc:  # noqa: BLE001
        # 其它任何异常（超时、连接断开……）都视为结果不明。
        return m.ATTEMPT_INCONCLUSIVE, None, _error_payload(exc)

