# durable_saga

进程退出后仍可恢复、确认的有序异步步骤（Saga）库。为仪器校准这类
"先取原参数 → 写新参数 → 读验证样本；失败则恢复原参数"的流程设计，
供采集服务直接嵌入。

- **纯标准库**：`sqlite3` + `asyncio`，无 Redis、消息代理或云工作流；
- **崩溃后能确认**：进程在"控制器已接受新参数、执行库尚未记录"之间
  被杀，重启后不会重复发起操作，也不会误判成功，而是用**同一操作身份**
  向适配器核实最初回执；
- **每步可独立核对**：每个步骤、每个恢复动作都有独立的状态、结果、
  失败信息、操作身份与尝试次数，外加只追加的事件流水——运行不止一个
  成功/失败布尔值；
- **跨进程一致**：两个采集进程连同一执行记录时，租约 + fencing epoch
  保证同一运行只有一套有效进展；
- **流程版本固化**：运行开始时的步骤定义（引用名/参数/顺序）随运行
  快照持久化；服务升级后旧运行仍按启动时的引用解析，缺函数则明确
  报告 `incompatible`，绝不换成同名新函数继续。

## 核心语义：三种"不确定"

| 步骤状态 | 含义 | 恢复动作 |
|---|---|---|
| `pending` | 从未发起 | 可以安全执行 |
| `started` | 已向控制器发起，本库未拿到确认（进程可能在此间被杀） | 用同一 `op_id` 查/重试，拿最初回执 |
| `unknown` | 尝试确认后仍无定论（如适配器离线） | 运行停在 `blocked`，适配器恢复后继续 |

`started` 既不能当失败重来（会产生第二次外部效果），也不能当成功
继续后续步骤——必须先查清外部真相。查清后是 `succeeded` 或 `failed`
（明确未执行）。

运行状态：`running` / `compensating` 为进行中（可能 `is_blocked`）；
`completed` / `rolled_back` / `cancelled` 为终态；`incompatible` 表示
当前部署缺少该运行所需的处理函数，**补齐兼容函数后可 resume**。

## 适配器约定（不要求跨库事务）

执行库与控制器各自有独立存储，**不共享数据库事务**。适配器只需保证：

1. 每次操作用库提供的 `ctx.op_id` 作为幂等键；
2. 同一 `op_id` 的重复提交/查询返回**同一次**操作的最初结果，
   不产生第二次外部效果。

## 接口速览

```python
from durable_saga import Engine, Store, Registry, Step, StepFailed

reg = Registry()
reg.handler("capture", capture_fn)            # 处理函数
reg.handler("apply",   apply_fn)
reg.handler("apply.resolve", resolve_fn)      # 结果不明时的查询函数
reg.handler("apply.restore", restore_fn)      # 恢复函数
reg.handler("verify",  verify_fn)

reg.define("calibration", 1, [
    Step("capture", "capture"),
    Step("apply", "apply",
         resolver_ref="apply.resolve",
         compensation_ref="apply.restore"),
    Step("verify", "verify"),
])

engine = Engine(Store("/data/runs.db"), reg, lease_ttl_seconds=30)
handle = await engine.start("calibration", params={"new_gain": 5.0})
view   = await engine.open(handle.run_id)     # 任何时候只读重开
```

处理函数签名：`async def fn(ctx: StepContext, step_params, results)`，
其中 `results` 是此前各步**已确认**结果的字典（恢复所需内容来自这里，
不是重启后重新读已改变的仪器）。恢复函数第三个参数是**本步**已确认
结果。

- 明确业务失败（如验证超差）：`raise StepFailed(msg, detail)` →
  已生效步骤按依赖逆序恢复，未执行步骤不再继续；
- 发起了但拿不到结论：`return AmbiguousOutcome(reason=...)` →
  调 resolver，或用同一 op_id 重试；
- resolver 返回 `Committed(result)` / `NotExecuted()` / `Unknown(reason)`。

服务重启后：

```python
await engine.recover_all()       # 推进所有未结束且租约空闲的运行
```

取消是持久化意图，不是取消协程：

```python
await engine.request_cancel(run_id)   # 登记意图
await engine.cancel(run_id)           # 并立即驱动：已生效操作会被撤回
```

## 并发保护

- **租约**：`resume` 先取租约；存在未过期租约时得到 `RunBusyError`
  （不是失败，稍后重试或等过期接管）；
- **fencing epoch**：过期接管会使 epoch 递增，旧持有者之后的所有写入
  被 `LeaseLostError` 拒绝，不会出现"一边向前写、一边按过时记录恢复"；
- 租约用心跳续租；心跳停止（进程死亡）后租约到期，新进程接管。

## 运行示例（校准适配器）

`examples/calibration/` 下有一个独立存储的仪器控制器
（`controller.db`，含参数与操作台账），执行库使用独立的 `runs.db`。
五个可运行场景：

```bash
cd examples/calibration

# 1. 进程在控制器已接受新参数、执行库确认前被杀，重启后用同 op_id 核实
python3 scenario_kill_recover.py

# 2. 验证失败按序恢复；恢复动作自身失败后 blocked，适配器恢复后续完
python3 scenario_rollback.py

# 3. 取消语义：开始前取消无任何外部操作；生效后取消真正恢复原参数
python3 scenario_cancel.py

# 4. 两个进程同一记录：租约拒绝并发，fencing 拒绝旧持有者迟到写入
python3 scenario_concurrency.py

# 5. 服务升级：旧运行缺处理函数时 incompatible，补齐后按旧语义完成
python3 scenario_versioning.py
```

每个场景结束都会打印：各步骤/恢复动作的状态与 op_id、最终参数、
控制器实际增益，以及完整的只追加事件流水。工作目录为
`workdir_demo/<场景>/`，进程被杀后可直接检查两个独立数据库。

## 测试

```bash
python3 -m unittest discover -s tests
```

`tests/test_cross_process.py` 会真正启动子进程并 `os._exit(42)`，
随后由新进程恢复，验证跨进程崩溃语义。

## 目录

```
durable_saga/
  contracts.py   # Step/Flow/Registry，handler/compensation/resolver 约定
  store.py       # SQLite：runs/steps/compensations/events + 租约/fence
  engine.py      # 异步状态机：向前推进、不确定解析、逆序补偿
  views.py       # RunView/StepView/CompensationView 只读核对视图
  errors.py
examples/calibration/
  controller.py             # 独立存储的仪器适配器（op_id 台账+故障注入）
  flow.py                   # 校准流程注册
  scenario_*.py             # 五个可运行场景
tests/
```
