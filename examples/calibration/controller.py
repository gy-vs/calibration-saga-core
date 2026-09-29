"""示例：一个带独立持久存储的校准仪器控制器适配器。

与执行库刻意使用**不同的 SQLite 文件**：

* ``controller.db`` 只属于仪器，记录真实参数与每次操作的
  身份/回执（操作台账）；
* 执行库的 ``runs.db`` 记录流程的执行事实。

二者不共享事务。控制器遵守幂等键约定：同一 ``op_id`` 的重复提交或
查询返回同一次操作的最初结果，绝不产生第二次外部效果。

仪器模型
========

* 仪器只有一个校准参数 ``gain``；
* ``apply`` 把增益改为目标值，回执包含修改前的值；
* ``restore`` 把增益恢复到回执中的原值；
* ``read_sample`` 返回 ``gain * 10`` 的“信号”，供验证步骤判定；
* 信号合法区间 ``[0, 100]``。

故障注入（仅为演示进程被杀/适配器故障）
--------------------------------------

* ``set_offline(True)`` 后，写参数/恢复操作失败；查询与台账仍可读；
* ``arm_kill_before_response(step)`` 后，指定步骤的下次操作会先
  持久化“已接受”事实，随后进程立即 ``os._exit``——模拟控制器
  已采用新参数、而执行进程没来得及记录成功。
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS instrument (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    gain REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger (
    op_id     TEXT PRIMARY KEY,
    op_type   TEXT NOT NULL,
    status    TEXT NOT NULL,           -- accepted
    request   TEXT NOT NULL,
    receipt   TEXT NOT NULL,
    seq       INTEGER NOT NULL,
    at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class ControllerUnavailable(RuntimeError):
    """适配器当前不可用（演示用：离线状态）。"""


class InstrumentController:
    """同步控制器；示例流程用 asyncio.to_thread 包装调用。"""

    def __init__(self, db_path: str, *, default_gain: float = 1.0):
        self.db_path = db_path
        self._init(default_gain)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        # 控制器使用 DELETE 日志：每个写事务在提交时把页写回主库并
        # 删除回滚日志，提交完成后即使进程被 SIGKILL，事实也已在主库中。
        # （执行库使用自己独立的 SQLite 文件与 WAL，两者不共享存储。）
        conn.execute("PRAGMA journal_mode=DELETE")
        return conn

    def _init(self, default_gain: float) -> None:
        with self._conn() as conn:
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO instrument (id, gain) VALUES (1, ?)",
                (default_gain,),
            )

    # -- 故障注入开关（自身也持久化，进程被杀后仍然保持） -----------------

    def set_offline(self, offline: bool) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO settings(key, value) VALUES ('offline', ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                ("1" if offline else "0",),
            )

    def is_offline(self) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'offline'"
            ).fetchone()
        return bool(row and row["value"] == "1")

    def arm_kill_before_response(self, op_type: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO settings(key, value) VALUES ('kill_before', ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (op_type,),
            )

    def arm_offline_after(self, op_type: str) -> None:
        """演示用：指定操作提交后，仪器立即进入（持久）离线状态。"""

        with self._conn() as conn:
            conn.execute(
                "INSERT INTO settings(key, value) VALUES ('offline_after', ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (op_type,),
            )

    def _take_offline_arm_standalone(self, op_type: str) -> bool:
        """提交后独立事务处理：命中则把仪器置为持久离线。"""

        hit = False
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'offline_after'"
            ).fetchone()
            if row and row["value"] == op_type:
                conn.execute("DELETE FROM settings WHERE key = 'offline_after'")
                conn.execute(
                    "INSERT INTO settings(key, value) VALUES ('offline', '1')"
                    " ON CONFLICT(key) DO UPDATE SET value = '1'"
                )
                hit = True
        return hit

    def take_kill_arm(self, op_type: str) -> bool:
        """取出（一次性）针对该操作类型的“提交后自杀”标记。

        用独立连接、在调用方写事务提交之后执行，确保死亡不会回滚
        “控制器已接受”的事实。
        """

        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'kill_before'"
            ).fetchone()
            if row and row["value"] == op_type:
                conn.execute("DELETE FROM settings WHERE key = 'kill_before'")
                return True
        return False

    # -- 只读查询 ---------------------------------------------------------

    def current_gain(self) -> float:
        with self._conn() as conn:
            return float(
                conn.execute("SELECT gain FROM instrument WHERE id = 1")
                .fetchone()["gain"]
            )

    def lookup(self, op_id: str) -> dict[str, Any] | None:
        """按操作身份查询最初回执；不存在返回 None（该操作确实没发生）。"""

        with self._conn() as conn:
            row = conn.execute(
                "SELECT op_id, op_type, status, request, receipt FROM ledger"
                " WHERE op_id = ?",
                (op_id,),
            ).fetchone()
        if row is None:
            return None
        import json

        return {
            "op_id": row["op_id"],
            "op_type": row["op_type"],
            "status": row["status"],
            "request": json.loads(row["request"]),
            "receipt": json.loads(row["receipt"]),
        }

    def ledger(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT op_id, op_type, status, receipt FROM ledger ORDER BY seq"
            ).fetchall()
        import json

        return [
            {
                "op_id": r["op_id"],
                "op_type": r["op_type"],
                "status": r["status"],
                "receipt": json.loads(r["receipt"]),
            }
            for r in rows
        ]

    # -- 幂等写操作 -------------------------------------------------------

    def _write_accepted(
        self, conn, op_id: str, op_type: str, request: dict, receipt: dict
    ) -> None:
        import json
        from datetime import datetime, timezone

        nxt = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM ledger"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO ledger (op_id, op_type, status, request, receipt,"
            " seq, at) VALUES (?, ?, 'accepted', ?, ?, ?, ?)",
            (op_id, op_type, json.dumps(request), json.dumps(receipt),
             nxt, datetime.now(timezone.utc).isoformat(timespec="microseconds")),
        )

    def apply(self, op_id: str, new_gain: float) -> dict[str, Any]:
        """应用新增益。同一 op_id 重复调用返回同一份回执。"""

        import json

        with self._conn() as conn:
            existing = conn.execute(
                "SELECT receipt FROM ledger WHERE op_id = ?", (op_id,)
            ).fetchone()
            if existing is not None:
                return json.loads(existing["receipt"])
            if self.is_offline():
                raise ControllerUnavailable("控制器离线：无法写入增益")

            old_gain = float(
                conn.execute("SELECT gain FROM instrument WHERE id = 1")
                .fetchone()["gain"]
            )
            receipt = {
                "op_id": op_id,
                "old_gain": old_gain,
                "new_gain": float(new_gain),
            }
            self._write_accepted(
                conn, op_id, "apply", {"new_gain": float(new_gain)}, receipt
            )
            conn.execute(
                "UPDATE instrument SET gain = ? WHERE id = 1",
                (float(new_gain),),
            )
        # with 退出（autocommit 下语句已逐条落盘）后再处理故障注入：
        # 保证控制器“已接受新参数”的事实先于进程死亡/离线持久化。
        if self.take_kill_arm("apply"):
            os._exit(42)
        if self._take_offline_arm_standalone("apply"):
            # apply 已生效，随后的 restore 将因离线失败。
            pass
        return receipt

    def restore(self, op_id: str, gain: float) -> dict[str, Any]:
        """恢复增益到指定值。同一 op_id 幂等。"""

        import json

        with self._conn() as conn:
            existing = conn.execute(
                "SELECT receipt FROM ledger WHERE op_id = ?", (op_id,)
            ).fetchone()
            if existing is not None:
                return json.loads(existing["receipt"])
            if self.is_offline():
                raise ControllerUnavailable("控制器离线：无法恢复增益")
            before = float(
                conn.execute("SELECT gain FROM instrument WHERE id = 1")
                .fetchone()["gain"]
            )
            receipt = {
                "op_id": op_id,
                "restored_from": before,
                "restored_to": float(gain),
            }
            self._write_accepted(
                conn, op_id, "restore", {"gain": float(gain)}, receipt
            )
            conn.execute(
                "UPDATE instrument SET gain = ? WHERE id = 1", (float(gain),)
            )
        return receipt

    def read_sample(self, op_id: str | None = None) -> dict[str, Any]:
        """读取验证样本。只读，离线也允许。"""

        gain = self.current_gain()
        return {"signal": gain * 10.0, "gain": gain, "valid": 0 <= gain * 10 <= 100}
