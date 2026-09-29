"""小型校准适配器示例：一个有独立持久存储的"仪器控制器"。

与执行库的关键区别（演示重点）
------------------------------

* 控制器用**自己的** SQLite 文件（``controller_state.db``），与执行库的
  执行记录（``runs.db``）完全分开。它们不参加、也不可能参加同一个数据库事务。
* 每个操作都有调用方给定的身份 ``op_id``。控制器在应用外部效果的同一时刻
  持久化回执；之后用同一 ``op_id`` 重复查询或重试，只会拿到**最初那次**的
  回执，不会再次应用效果。这正是执行库恢复所依赖的约定。
* 可以注入两类故障：

    - ``write_lost_response``：参数已生效并持久化，但回执在返回途中丢失，
      调用方看到的是超时（结果不明）；
    - ``kill_after_write`` / ``kill_barrier``：参数已生效、回执已落盘之后，
      进程被 SIGKILL——执行库还没来得及记录成功。

* 恢复（restore）可配置前 N 次明确拒绝（``restore_reject_times``），用来演示
  恢复动作自身失败、等待适配器恢复后继续的场景。
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import uuid
from typing import Any, Dict, Optional


# 操作名
OP_READ_ORIGINAL = "read_original"
OP_WRITE_PARAMS = "write_parameters"
OP_VALIDATE = "validate"
OP_RESTORE = "restore_parameters"


class ControllerError(Exception):
    """控制器明确拒绝（确定没有外部效果）。"""


class ControllerTimeout(Exception):
    """回执丢失 / 超时：操作是否生效未知。"""


class FakeInstrumentController:
    """用独立 SQLite 文件模拟的仪器控制器。

    :param db_path: 控制器自己的持久存储路径（务必与执行库 db 不同）。
    :param scenario_path: 场景配置 JSON 路径，控制器在每次调用时重新读取，
        方便演示脚本在运行中注入故障；计数器写在控制器 db 中，跨进程生效。
    """

    def __init__(self, db_path: str, scenario_path: Optional[str] = None) -> None:
        self.db_path = db_path
        self.scenario_path = scenario_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS params (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS operations (
                    op_id    TEXT PRIMARY KEY,
                    kind     TEXT NOT NULL,
                    payload  TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS counters (
                    name TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
                """
            )
            row = conn.execute("SELECT COUNT(*) AS n FROM params").fetchone()
            if row["n"] == 0:
                conn.execute(
                    "INSERT INTO params(id, payload) VALUES (1, ?)",
                    (json.dumps({"gain": 1.0, "offset": 0.0}),),
                )

    def _scenario(self) -> Dict[str, Any]:
        if not self.scenario_path or not os.path.exists(self.scenario_path):
            return {}
        try:
            with open(self.scenario_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, json.JSONDecodeError):
            return {}

    def _bump_counter(self, conn, name: str) -> int:
        conn.execute(
            "INSERT INTO counters(name, value) VALUES (?, 1) "
            "ON CONFLICT(name) DO UPDATE SET value = value + 1",
            (name,),
        )
        row = conn.execute(
            "SELECT value FROM counters WHERE name=?", (name,)
        ).fetchone()
        return int(row["value"])

    def _lookup_operation(self, conn, op_id: str):
        return conn.execute(
            "SELECT * FROM operations WHERE op_id=?", (op_id,)
        ).fetchone()

    # ------------------------------------------------------------------
    # 同步内核（每个调用开独立连接，保证跨进程可见）
    # ------------------------------------------------------------------

    def _read_params(self, conn) -> Dict[str, Any]:
        return json.loads(
            conn.execute("SELECT payload FROM params WHERE id=1").fetchone()["payload"]
        )

    def _store_operation(self, conn, op_id: str, kind: str,
                         payload: Dict[str, Any], at: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO operations(op_id, kind, payload, applied_at)"
            " VALUES (?, ?, ?, ?)",
            (op_id, kind, json.dumps(payload, ensure_ascii=False), at),
        )

    def _do_read_original(self, op_id: str) -> Dict[str, Any]:
        with self._connect() as conn:
            existing = self._lookup_operation(conn, op_id)
            if existing:
                return json.loads(existing["payload"])
            params = self._read_params(conn)
            receipt = {
                "op_id": op_id,
                "op": OP_READ_ORIGINAL,
                "params": params,
            }
            self._store_operation(conn, op_id, OP_READ_ORIGINAL, receipt,
                                  _now_iso())
            return receipt

    def _do_write_parameters(self, op_id: str, new_params: Dict[str, Any]) -> Dict[str, Any]:
        scenario = self._scenario()
        kill_after = bool(scenario.get("kill_after_write"))
        barrier = scenario.get("kill_barrier")
        lost = bool(scenario.get("write_lost_response"))

        with self._connect() as conn:
            existing = self._lookup_operation(conn, op_id)
            if existing:
                # 幂等约定：同一身份重复调用，返回最初回执，不重复应用。
                receipt = json.loads(existing["payload"])
                receipt["replayed"] = True
                return receipt

            receipt = {
                "op_id": op_id,
                "op": OP_WRITE_PARAMS,
                "accepted": True,
                "params": dict(new_params),
                "receipt_no": uuid.uuid4().hex[:12],
            }
            conn.execute(
                "UPDATE params SET payload=? WHERE id=1",
                (json.dumps(new_params),),
            )
            self._store_operation(conn, op_id, OP_WRITE_PARAMS, receipt,
                                  _now_iso())
            # 关键提交点：外部效果与回执在同一事务原子落盘，之后才可能 kill。
            conn.commit()

        if barrier:
            # 给外部（演示脚本的父进程）一个"回执已落盘、库尚未记录"的观察点。
            try:
                with open(barrier, "w", encoding="utf-8") as fh:
                    fh.write(json.dumps(receipt))
            except OSError:
                pass

        if kill_after:
            # 模拟进程在记录成功之前被终止：SIGKILL，没有机会执行任何清理。
            os.kill(os.getpid(), signal.SIGKILL)

        if lost:
            raise ControllerTimeout(
                f"op {op_id}: 控制器已接受参数但应答丢失（结果不明）")

        return receipt

    def _do_validate(self, op_id: str, expected: Dict[str, Any]) -> Dict[str, Any]:
        scenario = self._scenario()
        force_fail = bool(scenario.get("force_validation_fail"))

        with self._connect() as conn:
            existing = self._lookup_operation(conn, op_id)
            if existing:
                receipt = json.loads(existing["payload"])
                receipt["replayed"] = True
                return receipt
            current = self._read_params(conn)
            passed = (current == expected) and not force_fail
            receipt = {
                "op_id": op_id,
                "op": OP_VALIDATE,
                "passed": passed,
                "current_params": current,
                "expected_params": expected,
            }
            self._store_operation(conn, op_id, OP_VALIDATE, receipt, _now_iso())
            return receipt

    def _do_restore(self, op_id: str, original: Dict[str, Any]) -> Dict[str, Any]:
        scenario = self._scenario()
        reject_times = int(scenario.get("restore_reject_times", 0))

        with self._connect() as conn:
            existing = self._lookup_operation(conn, op_id)
            if existing:
                receipt = json.loads(existing["payload"])
                receipt["replayed"] = True
                return receipt

            if reject_times:
                attempts = self._bump_counter(conn, "restore_rejections")
                conn.commit()
                if attempts <= reject_times:
                    raise ControllerError(
                        f"恢复被拒绝（{attempts}/{reject_times}）：仪器暂时繁忙")

            receipt = {
                "op_id": op_id,
                "op": OP_RESTORE,
                "restored": True,
                "params": dict(original),
            }
            conn.execute(
                "UPDATE params SET payload=? WHERE id=1",
                (json.dumps(original),),
            )
            self._store_operation(conn, op_id, OP_RESTORE, receipt, _now_iso())
            return receipt

    # ------------------------------------------------------------------
    # 对外的 async 接口（适配器示例）
    # ------------------------------------------------------------------

    async def read_original(self, op_id: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._do_read_original, op_id)

    async def write_parameters(self, op_id: str,
                               new_params: Dict[str, Any]) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self._do_write_parameters, op_id, new_params)

    async def validate(self, op_id: str,
                       expected: Dict[str, Any]) -> Dict[str, Any]:
        return await asyncio.to_thread(self._do_validate, op_id, expected)

    async def restore_parameters(self, op_id: str,
                                  original: Dict[str, Any]) -> Dict[str, Any]:
        return await asyncio.to_thread(self._do_restore, op_id, original)

    def current_params_sync(self) -> Dict[str, Any]:
        """供演示脚本旁路核对仪器真实状态（不进入校准操作身份体系）。"""

        with self._connect() as conn:
            return self._read_params(conn)


def _now_iso() -> str:
    import datetime as dt
    return dt.datetime.now(dt.timezone.utc).isoformat()
