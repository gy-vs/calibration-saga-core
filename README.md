# durable-calibration

一个可直接嵌入采集服务的**持久化校准流程编排库**。只依赖 Python 标准库
（`asyncio` + `sqlite3`），不引入 Redis、消息代理或云端工作流服务，也不是
任务队列、命令行产品或工作流编辑器。

它解决的核心问题是：**服务进程退出（包括被 SIGKILL）之后，仍然能确认哪些
对仪器的操作已经发生，并继续验证或完成尚未结束的恢复动作。**

```python
from durable_calibration import CalibrationLibrary, Flow, FlowStep

lib = CalibrationLibrary("./data/calibration.db")
lib.register_handlers(read_original=..., write_parameters=...,
                      validate_sample=..., restore_original=...)
lib.register_flow(Flow.define("instrument-calibration", [...]))

run_id = await lib.start_run("instrument-calibration", run_input={...})
snap = await lib.run(run_id)

# —— 进程在任意时刻被杀后，用新进程、同一个 db 文件： ——
snap = await lib.resume(run_id)
```

## 它保证什么

### 1. 执行事实先于外部效果记录，崩溃后不靠猜

对控制器的每一次外部调用，库都**先在 SQLite 落一条 `started` 尝试
（带操作身份 `op_id`）**，再去调用控制器，最后在另一个短事务里落结果。

```
BEGIN IMMEDIATE → 写入 attempt(started, op_id) → COMMIT
                    …… 在事务外调用控制器（慢，可能被杀）……
BEGIN IMMEDIATE → 落 attempt 结果 + 推进步骤状态 → COMMIT
```

因此“控制器已经接受新参数、但进程在记录成功前被杀”这个具体场景下，重开
运行时记录只表明这一步**曾经开始**，而仪器实际已经是新参数。库不会：

- 把它当成明确失败而**重新生成一次操作**；
- 把它当成整体成功而**继续发布测量结果**。

### 2. 利用适配器的幂等回执约定完成恢复，不要求分布式事务

适配器（见 `examples/fake_controller.py`）对每次操作保存 `op_id` 和回执，
**用同一 `op_id` 重复查询或重试只返回最初那次的结果，不会重复应用效果**。
执行库自己的执行记录（`runs.db`）与控制器存储（`controller_state.db`）
是两份完全独立的 SQLite，从不参加同一个数据库事务。

崩溃恢复时，库领取到的是**同一个 `op_id` 的 replay 动作**（`ctx.replay=True`），
由适配器的幂等约定把“结果不明”变成“确认成功/确认失败”。事实链里会有一条
`attempt_replayed`。

### 3. 恢复原参数只来自已确认的执行结果

补偿函数签名是 `async def compensate(ctx, params, result)`，其中 `result`
是该执行步骤**已经确认**的结果，`ctx.results[key]` 是此前各步骤已确认的
结果。恢复所需内容（示例中的 `original_params`）在执行时就固化进了执行
记录；重启后即使仪器已经是新参数，也**不会再去读取已经改变的仪器状态**。

### 4. 每一步、每个恢复动作、每次尝试都能独立核对

运行不是单个成功/失败布尔值。四张表：

| 表 | 粒度 |
|---|---|
| `flow_runs` | 整次运行总体状态 |
| `flow_steps` | 每个步骤的执行状态、结果、错误、撤回状态、撤回错误 |
| `run_attempts` | 对控制器的**单次**尝试（started/succeeded/rejected/inconclusive），含 `op_id` |
| `run_facts` | append-only 的执行事实（顺序号、时间、步骤、尝试、明细） |

查询接口 `await lib.get_run(run_id)` 返回 `RunSnapshot`，每一步都有独立的
`execute_status` / `compensate_status` / `result` / `error`。

### 5. 验证失败：按依赖逆序撤回，未执行的校准不继续

某步骤的确认结果命中补偿判据（如验证步骤 `passed == False`）时，成功落库
与“整体转入撤回”在**同一事务**内发生：尚未执行的前向步骤被标记 `skipped`，
已生效步骤按**逆序**调用各自的补偿函数。

### 6. 恢复动作自身也可能失败：明确报告“仪器还没恢复到什么状态”

- 恢复操作**结果不明**（超时/连接断开）→ 步骤 `compensate_status=inconclusive`，
  运行停在 `compensation_blocked`，`blocked_reason` 指明卡在哪个步骤、
  对应哪个 `op_id`。适配器恢复后 `resume` 用**同一 op_id**继续。
- 恢复被**明确拒绝**（控制器返回确定失败，确定没恢复成）→ 下一轮用
  **新 op_id** 再次尝试，直到成功。

两类失败的区分由处理函数抛出 `OperationRejected`（确定无外部效果）来表达；
其它一切异常都视为“结果不明”。

### 7. 取消协程 ≠ 仪器已回原参数

`await lib.request_cancel(run_id)` 只在执行记录里持久一个取消标记。是否真的
取消、仪器是否回到原参数，仍由随后的 `resume` 驱动撤回流程来**确认**。
取消 Python 协程本身不产生、也不代表任何外部回滚效果。若取消时没有任何
已确认生效的操作，终态是 `cancelled`；否则完成撤回后是 `compensated`。

### 8. 正常完成的结果在重启后不重写、不撤回

`succeeded` / `compensated` / `cancelled` 是终态。对终态运行再次 `resume`
直接返回快照，不产生任何新的控制器操作。

### 9. 多个采集进程 / 恢复早于旧进程确认退出：只有一套有效进展

- 每个运行由带 TTL 的**租约**保护：未过期时第二个驱动者得到 `RunLocked`。
- 每次写事务都做 fencing 校验（`lock_owner` + 单调递增的 `lock_epoch`）。
  确认旧进程已死且租约过期后，新进程 `resume(force=True)` 接管，epoch 递增；
  旧进程即使残留，它随后的任何提交都会被数据库拒绝（`RunLockLost`）。
- 因此不会出现“一边按新记录写新参数，一边按过时记录执行恢复”的双写。

### 10. 流程随服务升级变化：旧运行永远按它开始时的定义解释

`start_run` 时整份流程定义（步骤顺序、处理函数名、参数、补偿判据）固化为
JSON 快照存进该运行。处理函数按**名字**在当前库实例中解析。服务升级后：

- 旧运行仍按旧快照的步骤含义恢复；
- 若某个当时的处理/恢复函数在新版本里已不存在，运行明确停在阻塞态，
  `blocked_reason.code == "handler_unavailable"` 并给出步骤和函数名，
  **不会**被替换成同名的新函数继续跑。
- 同一 `(name, version)` 不允许以不同定义重复登记（`FlowVersionMismatch`）；
  含义变更请使用新版本号。

## 运行状态一览

```
pending → running → succeeded
                    ↓（结果不明）
                awaiting_confirmation
running →（验证失败 / 明确拒绝 / 取消）→ compensating → compensated
                                            ↓（恢复结果不明）
                                    compensation_blocked
无任何已生效操作时取消 → cancelled
```

## 处理函数约定

```python
async def handler(ctx: HandlerContext, params: dict) -> Any:
    # 必须用 ctx.op_id 调用控制器，不要自己生成操作身份。
    receipt = await controller.write(ctx.op_id, params["..."])
    return receipt            # 返回值必须可 JSON 化；不能保存视为结果不明

async def compensator(ctx: HandlerContext, params: dict, result: Any) -> Any:
    # result = 该执行步骤已确认的结果；恢复内容只能来自它 / ctx.results。
    await controller.restore(ctx.op_id, result["original_params"])
```

- 返回值会作为该步骤确认结果持久化，并可被后续步骤经
  `ctx.results["step_key"]` 使用。
- 抛出 `OperationRejected(msg, receipt=...)` 表示控制器明确拒绝、确定无
  外部效果；其它异常一律按结果不明处理。
- `ctx.replay` 为 `True` 表示这是崩溃后的再确认，应仍用同一 `ctx.op_id`
  走适配器的查询/重试接口。

## 运行示例（真实 SIGKILL）

示例里的控制器回执与执行库使用**不同的持久存储**：

```
examples/fake_controller.py   # 控制器：自己的 controller_state.db + 幂等回执
examples/calibration_flow.py  # 步骤处理/恢复函数与流程登记
examples/worker.py            # 子进程：start / resume / cancel / inspect
examples/demo.py              # 一键演示（会真正杀掉子进程）
```

```bash
pip install -e .
python examples/demo.py
```

演示包含三个场景：验证成功看最终参数；写入后进程被杀、新进程用同一
`op_id` 查到原回执继续到成功；验证失败（恢复动作先被拒绝一次）后仪器
回到原参数。每步都打印执行事实与仪器的真实参数。

## 运行测试

```bash
python -m unittest discover -s tests -v
```

`tests/test_core.py` 覆盖状态机/崩溃再确认/撤回/取消/版本化/租约；
`tests/test_integration_kill.py` 用真实子进程 + SIGKILL 验证跨进程恢复。

## 存储位置与并发布局

- 多个采集进程应指向**同一个**执行记录文件（如网络盘上的
  `calibration.db`），SQLite 以 WAL 模式打开，`busy_timeout=10s`。
- 每个库实例维护自己的连接；跨进程互斥由库内租约 + fencing 保证，
  不依赖调用方加外部锁。
- 执行记录文件是唯一的协调点；控制器存储归适配器所有，库不直接触碰。
