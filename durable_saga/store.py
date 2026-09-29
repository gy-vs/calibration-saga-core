"""SQLite 持久化层（同步实现，由 Engine 放到线程池中调用）。

设计要点
========

* 所有涉及外部效果前后的状态推进都是**独立短事务**，绝不在等待
  控制器的事务期间持有数据库锁，因此进程在任意时刻被 ``SIGKILL``，
  落盘内容都自洽。
* 租约（lease）+ fencing epoch：同一运行同一时刻只可能有一个
  驱动者；旧持有者即使还活着，其 epoch 过期后所有带 fence 的
  写入都会被 :class:`LeaseLostError` 拒绝，不可能一边向前写入、
  一边按过时记录恢复。
* 步骤、恢复动作各有独立行，事件表只追加——恢复时看到“started”
  即表示“曾发起、未确认”，必须用同一操作身份去适配器核实。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import (
    IncompatibleRunError,
    RunBusyError,
    RunNotFoundError,
)
from .views import (
    CompensationView,
    EventView,
    RunView,
    StepView,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id            TEXT PRIMARY KEY,
    flow_name         TEXT NOT NULL,
    flow_version      INTEGER NOT NULL,
    state             TEXT NOT NULL,
    snapshot          TEXT NOT NULL,
    params            TEXT,
    result            TEXT,
    failure           TEXT,
    cancel_requested  INTEGER NOT NULL DEFAULT 0,
    blocked_reason    TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    lease_owner       TEXT,
    lease_epoch       INTEGER NOT NULL DEFAULT 0,
    lease_expires_at  TEXT
);

CREATE TABLE IF NOT EXISTS steps (
    run_id           TEXT NOT NULL REFERENCES runs(run_id),
    seq              INTEGER NOT NULL,
    name             TEXT NOT NULL,
    handler_ref      TEXT NOT NULL,
    compensation_ref TEXT,
    resolver_ref     TEXT,
    params           TEXT,
    state            TEXT NOT NULL,
    result           TEXT,
    error            TEXT,
    op_id            TEXT,
    attempt          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, name)
);

CREATE TABLE IF NOT EXISTS compensations (
    run_id           TEXT NOT NULL REFERENCES runs(run_id),
    step_name        TEXT NOT NULL,
    seq              INTEGER NOT NULL,
    compensation_ref TEXT NOT NULL,
    state            TEXT NOT NULL,
    op_id            TEXT,
    result           TEXT,
    error            TEXT,
    attempt          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, step_name)
);

CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id    TEXT NOT NULL REFERENCES runs(run_id),
    seq       INTEGER NOT NULL,
    at        TEXT NOT NULL,
    kind      TEXT NOT NULL,
    step_name TEXT,
    payload   TEXT,
    UNIQUE (run_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_steps_run ON steps(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_comp_run ON compensations(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at);
"""


class LeaseLostError(Exception):
    """fence 检查失败：当前持有者的租约已失效（被更新的持有者取代）。

    引擎捕获后立即停止本次驱动，不做任何进一步外部调用。
    """


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def loads(text: str | None) -> Any:
    return None if text is None else json.loads(text)


def error_json(exc: BaseException | str, detail: Any = None) -> dict[str, Any]:
    if isinstance(exc, BaseException):
        from .contracts import StepFailed

        d = getattr(exc, "detail", None)
        return {
            "type": type(exc).__name__,
            "message": str(exc),
            "detail": d if d is not None else detail,
            "_step_failed": isinstance(exc, StepFailed),
        }
    return {"type": "Error", "message": str(exc), "detail": detail}


class Store:
    """线程内同步的 SQLite 封装。每个公共方法使用独立短连接。"""

    def __init__(self, path: str, now_fn=utcnow_iso, busy_timeout_ms: int = 5000,
                 journal_mode: str = "WAL"):
        self.path = path
        self.now = now_fn
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_mode = journal_mode
        self.init()

    # -- 连接与事务 -------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,  # autocommit；事务由我们显式控制
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA journal_mode={self.journal_mode}")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={int(self.busy_timeout_ms)}")
        return conn

    @contextmanager
    def transaction(self, conn: sqlite3.Connection):
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def init(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)

    # -- 小工具 -----------------------------------------------------------

    def _row(self, conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
        cur = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        return cur.fetchone()

    def require_row(self, run_id: str) -> sqlite3.Row:
        with closing(self._connect()) as conn:
            row = self._row(conn, run_id)
        if row is None:
            raise RunNotFoundError(f"运行不存在: {run_id}")
        return row

    def _fenced(
        self, conn: sqlite3.Connection, run_id: str, owner: str, epoch: int
    ) -> None:
        """事务内校验租约归属（fence）。"""

        cur = conn.execute(
            "SELECT lease_owner, lease_epoch FROM runs WHERE run_id = ?",
            (run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise RunNotFoundError(f"运行不存在: {run_id}")
        if row["lease_owner"] != owner or row["lease_epoch"] != epoch:
            raise LeaseLostError(run_id)

    def _event(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        kind: str,
        step_name: str | None = None,
        payload: Any = None,
    ) -> None:
        """调用方必须已开启事务并通过 fence。"""

        nxt = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO events (run_id, seq, at, kind, step_name, payload)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, nxt, self.now(), kind, step_name, dumps(payload)),
        )

    def append_event(
        self,
        run_id: str,
        owner: str,
        epoch: int,
        kind: str,
        step_name: str | None = None,
        payload: Any = None,
    ) -> None:
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            self._event(conn, run_id, kind, step_name, payload)

    # -- 运行创建 ---------------------------------------------------------

    def create_run(
        self,
        run_id: str,
        flow_name: str,
        flow_version: int,
        snapshot: list[dict[str, Any]],
        params: Any,
    ) -> None:
        now = self.now()
        with closing(self._connect()) as conn, self.transaction(conn):
            if self._row(conn, run_id) is not None:
                raise ValueError(f"运行标识已存在: {run_id}")
            conn.execute(
                "INSERT INTO runs (run_id, flow_name, flow_version, state,"
                " snapshot, params, created_at, updated_at, lease_epoch)"
                " VALUES (?, ?, ?, 'running', ?, ?, ?, ?, 0)",
                (run_id, flow_name, flow_version, dumps(snapshot),
                 dumps(params), now, now),
            )
            for seq, s in enumerate(snapshot):
                conn.execute(
                    "INSERT INTO steps (run_id, seq, name, handler_ref,"
                    " compensation_ref, resolver_ref, params, state)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
                    (run_id, seq, s["name"], s["handler_ref"],
                     s.get("compensation_ref"), s.get("resolver_ref"),
                     dumps(s.get("params"))),
                )
            self._event(conn, run_id, "run_created", payload={
                "flow": flow_name, "version": flow_version,
                "steps": [s["name"] for s in snapshot],
            })

    def get_snapshot(self, run_id: str) -> list[dict[str, Any]]:
        row = self.require_row(run_id)
        return loads(row["snapshot"])

    # -- 租约 -------------------------------------------------------------

    def acquire_lease(
        self, run_id: str, owner: str, ttl_seconds: float
    ) -> int:
        """取得驱动租约，返回新的 epoch。

        运行处于终态时抛出 RunNotFoundError 之外由调用方先判断；
        存在未过期租约时抛出 :class:`RunBusyError`。
        """

        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat(timespec="microseconds")
        expires = (now_dt + timedelta(seconds=ttl_seconds)).isoformat(
            timespec="microseconds"
        )
        with closing(self._connect()) as conn, self.transaction(conn):
            row = self._row(conn, run_id)
            if row is None:
                raise RunNotFoundError(f"运行不存在: {run_id}")
            cur_owner, cur_exp = row["lease_owner"], row["lease_expires_at"]
            if cur_owner is not None and cur_exp is not None and cur_exp > now:
                raise RunBusyError(run_id, cur_owner, cur_exp)
            # 无论是首次占用还是过期接管，都递增 epoch，使所有旧持有者
            # 手里的 fence 立即作废。
            cur = conn.execute(
                "UPDATE runs SET lease_owner = ?, lease_epoch = lease_epoch + 1,"
                " lease_expires_at = ?, updated_at = ? WHERE run_id = ?",
                (owner, expires, now, run_id),
            )
            if cur.rowcount != 1:
                raise LeaseLostError(run_id)
            new_epoch = conn.execute(
                "SELECT lease_epoch FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()[0]
            self._event(conn, run_id, "lease_acquired", payload={
                "owner": owner, "epoch": new_epoch,
                "replaced": cur_owner,
            })
            return new_epoch

    def renew_lease(
        self, run_id: str, owner: str, epoch: int, ttl_seconds: float
    ) -> bool:
        expires = (
            datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        ).isoformat(timespec="microseconds")
        with closing(self._connect()) as conn, self.transaction(conn):
            cur = conn.execute(
                "UPDATE runs SET lease_expires_at = ?"
                " WHERE run_id = ? AND lease_owner = ? AND lease_epoch = ?",
                (expires, run_id, owner, epoch),
            )
            return cur.rowcount == 1

    def release_lease(self, run_id: str, owner: str, epoch: int) -> None:
        with closing(self._connect()) as conn, self.transaction(conn):
            conn.execute(
                "UPDATE runs SET lease_owner = NULL, lease_expires_at = NULL,"
                " updated_at = ?"
                " WHERE run_id = ? AND lease_owner = ? AND lease_epoch = ?",
                (self.now(), run_id, owner, epoch),
            )
            # 释放不到（租约已被接管）不算错误：本来就该静默退出。

    # -- 取消请求（不带 fence，任何调用方都可登记意图） --------------------

    def request_cancel(self, run_id: str, reason: str = "") -> bool:
        """登记取消意图。返回是否生效（终态运行返回 False）。"""

        with closing(self._connect()) as conn, self.transaction(conn):
            row = self._row(conn, run_id)
            if row is None:
                raise RunNotFoundError(f"运行不存在: {run_id}")
            if row["state"] in ("completed", "rolled_back", "cancelled"):
                return False
            if row["cancel_requested"]:
                return True
            conn.execute(
                "UPDATE runs SET cancel_requested = 1, updated_at = ?"
                " WHERE run_id = ?",
                (self.now(), run_id),
            )
            self._event(conn, run_id, "run_cancel_requested",
                        payload={"reason": reason})
            return True

    # -- 步骤状态推进（全部带 fence） --------------------------------------

    def mark_step_started(
        self, run_id, owner, epoch, step_name, op_id, attempt
    ) -> None:
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            cur = conn.execute(
                "UPDATE steps SET state = 'started', op_id = ?, attempt = ?,"
                " error = NULL"
                " WHERE run_id = ? AND name = ?",
                (op_id, attempt, run_id, step_name),
            )
            if cur.rowcount != 1:
                raise LeaseLostError(run_id)
            conn.execute(
                "UPDATE runs SET updated_at = ?, blocked_reason = NULL"
                " WHERE run_id = ?",
                (self.now(), run_id),
            )
            self._event(conn, run_id, "step_started", step_name,
                        {"op_id": op_id, "attempt": attempt})

    def resolve_step(
        self,
        run_id: str,
        owner: str,
        epoch: int,
        step_name: str,
        state: str,
        *,
        result: Any = None,
        error: Any = None,
        op_id: str | None = None,
        attempt: int | None = None,
    ) -> None:
        """把步骤落到确定状态 succeeded/failed/unknown，附结果或错误。"""

        assert state in ("succeeded", "failed", "unknown")
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            sets = ["state = ?", "result = ?", "error = ?"]
            vals: list[Any] = [state, dumps(result), dumps(error)]
            if op_id is not None:
                sets.append("op_id = ?")
                vals.append(op_id)
            if attempt is not None:
                sets.append("attempt = ?")
                vals.append(attempt)
            vals += [run_id, step_name]
            cur = conn.execute(
                f"UPDATE steps SET {', '.join(sets)} WHERE run_id = ? AND name = ?",
                vals,
            )
            if cur.rowcount != 1:
                raise LeaseLostError(run_id)
            conn.execute(
                "UPDATE runs SET updated_at = ? WHERE run_id = ?",
                (self.now(), run_id),
            )
            kind = {
                "succeeded": "step_succeeded",
                "failed": "step_failed",
                "unknown": "step_unknown",
            }[state]
            self._event(conn, run_id, kind, step_name, {
                "op_id": op_id, "result": result, "error": error,
            })

    def mark_blocked(
        self,
        run_id: str,
        owner: str,
        epoch: int,
        reason: str,
        step_name: str | None,
        detail: Any = None,
    ) -> None:
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            cur = conn.execute(
                "UPDATE runs SET blocked_reason = ?, updated_at = ?"
                " WHERE run_id = ?",
                (reason, self.now(), run_id),
            )
            if cur.rowcount != 1:
                raise LeaseLostError(run_id)
            self._event(conn, run_id, "run_blocked", step_name,
                        {"reason": reason, "detail": detail})

    # -- 进入补偿阶段 ------------------------------------------------------

    def begin_compensation(
        self,
        run_id: str,
        owner: str,
        epoch: int,
        reason: str,
        failure: Any,
    ) -> None:
        """原子地：运行转 compensating，未执行步骤标 skipped，为每个
        已成功步骤建立独立的恢复动作行。"""

        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            cur = conn.execute(
                "UPDATE runs SET state = 'compensating', failure = ?,"
                " blocked_reason = NULL, updated_at = ? WHERE run_id = ?",
                (dumps(failure), self.now(), run_id),
            )
            if cur.rowcount != 1:
                raise LeaseLostError(run_id)
            self._event(conn, run_id, "compensation_started",
                        payload={"reason": reason, "failure": failure})

            # 从未开始的步骤：确定不会再执行，标 skipped 并留痕。
            skipped = conn.execute(
                "UPDATE steps SET state = 'skipped'"
                " WHERE run_id = ? AND state = 'pending'",
                (run_id,),
            ).rowcount
            if skipped:
                for r in conn.execute(
                    "SELECT name FROM steps WHERE run_id = ? AND state = 'skipped'"
                    " ORDER BY seq",
                    (run_id,),
                ):
                    self._event(conn, run_id, "step_skipped", r["name"])

            # 为每个已成功步骤建立恢复动作行，逆序执行。
            for r in conn.execute(
                "SELECT name, seq, compensation_ref FROM steps"
                " WHERE run_id = ? AND state = 'succeeded' ORDER BY seq DESC",
                (run_id,),
            ):
                if r["compensation_ref"]:
                    conn.execute(
                        "INSERT INTO compensations (run_id, step_name, seq,"
                        " compensation_ref, state) VALUES (?, ?, ?, ?, 'pending')",
                        (run_id, r["name"], r["seq"], r["compensation_ref"]),
                    )
                    self._event(conn, run_id, "comp_pending", r["name"])
                else:
                    conn.execute(
                        "INSERT INTO compensations (run_id, step_name, seq,"
                        " compensation_ref, state) VALUES (?, ?, ?, ?, 'not_required')",
                        (run_id, r["name"], r["seq"], r["compensation_ref"] or ""),
                    )
                    self._event(conn, run_id, "comp_not_required", r["name"])

    def ensure_comp_not_occurred(
        self, run_id: str, owner: str, epoch: int, step_name: str
    ) -> None:
        """把一个确认未生效的步骤记录为无需撤回（必要时补建恢复行）。"""

        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            step = conn.execute(
                "SELECT seq, compensation_ref FROM steps"
                " WHERE run_id = ? AND name = ?",
                (run_id, step_name),
            ).fetchone()
            if step is None:
                return
            exists = conn.execute(
                "SELECT state FROM compensations WHERE run_id = ? AND step_name = ?",
                (run_id, step_name),
            ).fetchone()
            if exists is not None:
                if exists["state"] != "not_occurred":
                    conn.execute(
                        "UPDATE compensations SET state = 'not_occurred',"
                        " result = NULL, error = NULL"
                        " WHERE run_id = ? AND step_name = ?",
                        (run_id, step_name),
                    )
                    self._event(conn, run_id, "comp_not_occurred", step_name)
                return
            conn.execute(
                "INSERT INTO compensations (run_id, step_name, seq,"
                " compensation_ref, state) VALUES (?, ?, ?, ?, 'not_occurred')",
                (run_id, step_name, step["seq"], step["compensation_ref"] or ""),
            )
            self._event(conn, run_id, "comp_not_occurred", step_name)

    def ensure_compensation_for_step(
        self, run_id: str, owner: str, epoch: int, step_name: str
    ) -> None:
        """补偿阶段中确认某步骤此前已生效时，确保它有对应的恢复动作行。

        正常状态机下，``begin_compensation`` 已为所有 succeeded 步骤
        建行；此方法用于防御“结果不明跨越补偿阶段”的极端重启边界。
        """

        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            step = conn.execute(
                "SELECT compensation_ref FROM steps"
                " WHERE run_id = ? AND name = ? AND state = 'succeeded'",
                (run_id, step_name),
            ).fetchone()
            if step is None:
                return
            exists = conn.execute(
                "SELECT 1 FROM compensations WHERE run_id = ? AND step_name = ?",
                (run_id, step_name),
            ).fetchone()
            if exists:
                return
            seq = conn.execute(
                "SELECT seq FROM steps WHERE run_id = ? AND name = ?",
                (run_id, step_name),
            ).fetchone()[0]
            ref = step["compensation_ref"]
            if ref:
                conn.execute(
                    "INSERT INTO compensations (run_id, step_name, seq,"
                    " compensation_ref, state) VALUES (?, ?, ?, ?, 'pending')",
                    (run_id, step_name, seq, ref),
                )
                self._event(conn, run_id, "comp_pending", step_name)
            else:
                conn.execute(
                    "INSERT INTO compensations (run_id, step_name, seq,"
                    " compensation_ref, state) VALUES (?, ?, ?, ?, 'not_required')",
                    (run_id, step_name, seq, ref or ""),
                )
                self._event(conn, run_id, "comp_not_required", step_name)

    def mark_comp_started(
        self, run_id, owner, epoch, step_name, op_id, attempt
    ) -> None:
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            cur = conn.execute(
                "UPDATE compensations SET state = 'started', op_id = ?,"
                " attempt = ?, error = NULL WHERE run_id = ? AND step_name = ?",
                (op_id, attempt, run_id, step_name),
            )
            if cur.rowcount != 1:
                raise LeaseLostError(run_id)
            conn.execute(
                "UPDATE runs SET updated_at = ?, blocked_reason = NULL"
                " WHERE run_id = ?",
                (self.now(), run_id),
            )
            self._event(conn, run_id, "comp_started", step_name,
                        {"op_id": op_id, "attempt": attempt})

    def resolve_comp(
        self,
        run_id: str,
        owner: str,
        epoch: int,
        step_name: str,
        state: str,
        *,
        result: Any = None,
        error: Any = None,
    ) -> None:
        assert state in ("succeeded", "failed", "unknown", "not_occurred")
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            cur = conn.execute(
                "UPDATE compensations SET state = ?, result = ?, error = ?"
                " WHERE run_id = ? AND step_name = ?",
                (state, dumps(result), dumps(error), run_id, step_name),
            )
            if cur.rowcount != 1:
                raise LeaseLostError(run_id)
            conn.execute(
                "UPDATE runs SET updated_at = ? WHERE run_id = ?",
                (self.now(), run_id),
            )
            kind = {
                "succeeded": "comp_succeeded",
                "failed": "comp_failed",
                "unknown": "comp_unknown",
                "not_occurred": "comp_not_occurred",
            }[state]
            self._event(conn, run_id, kind, step_name,
                        {"result": result, "error": error})

    # -- 终态 --------------------------------------------------------------

    def complete_run(
        self, run_id, owner, epoch, result: Any, release: bool = True
    ) -> None:
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            conn.execute(
                "UPDATE runs SET state = 'completed', result = ?,"
                " blocked_reason = NULL,"
                " lease_owner = NULL, lease_expires_at = NULL,"
                " updated_at = ? WHERE run_id = ?",
                (dumps(result), self.now(), run_id),
            )
            self._event(conn, run_id, "run_completed", payload={"result": result})

    def finish_rollback(
        self, run_id, owner, epoch, cancelled: bool, failure: Any
    ) -> None:
        state = "cancelled" if cancelled else "rolled_back"
        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            conn.execute(
                "UPDATE runs SET state = ?, failure = COALESCE(failure, ?),"
                " blocked_reason = NULL, lease_owner = NULL,"
                " lease_expires_at = NULL, updated_at = ? WHERE run_id = ?",
                (state, dumps(failure), self.now(), run_id),
            )
            self._event(conn, run_id,
                        "run_cancelled" if cancelled else "run_rolled_back",
                        payload={"failure": failure})

    def mark_incompatible(self, run_id: str, missing: list[str]) -> None:
        """无对应处理函数：明确报告无法继续（可恢复，补齐函数后可 resume）。

        通常在未持租约或持租约时都可能调用；这里不加 fence，因为它
        表达的是“当前部署缺函数”这一事实，任何恢复入口都可确认。
        """

        with closing(self._connect()) as conn, self.transaction(conn):
            row = self._row(conn, run_id)
            if row is None:
                raise RunNotFoundError(f"运行不存在: {run_id}")
            if row["state"] == "incompatible":
                return
            conn.execute(
                "UPDATE runs SET state = 'incompatible', failure = ?,"
                " blocked_reason = ?, lease_owner = NULL,"
                " lease_expires_at = NULL, updated_at = ? WHERE run_id = ?",
                (dumps({"missing_refs": missing,
                        "message": "缺少运行开始时对应的处理函数引用"}),
                 f"缺少处理函数引用: {', '.join(missing)}",
                 self.now(), run_id),
            )
            self._event(conn, run_id, "run_incompatible",
                        payload={"missing_refs": missing})

    def clear_incompatible_if_ready(
        self, run_id: str, owner: str, epoch: int
    ) -> str:
        """持租约后调用：若运行处于 incompatible 且引用已补齐，恢复到
        中断前的阶段（running/compensating 记录在 failure 之外不可得，
        因此根据是否已存在补偿行判断）。返回当前 state。"""

        with closing(self._connect()) as conn, self.transaction(conn):
            self._fenced(conn, run_id, owner, epoch)
            row = self._row_conn(conn, run_id)
            if row["state"] != "incompatible":
                return row["state"]
            has_comp = conn.execute(
                "SELECT 1 FROM compensations WHERE run_id = ? LIMIT 1",
                (run_id,),
            ).fetchone()
            target = "compensating" if has_comp else "running"
            conn.execute(
                "UPDATE runs SET state = ?, blocked_reason = NULL,"
                " updated_at = ? WHERE run_id = ?",
                (target, self.now(), run_id),
            )
            self._event(conn, run_id, "run_compatible_resumed",
                        payload={"state": target})
            return target

    def _row_conn(self, conn, run_id: str):
        cur = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        return cur.fetchone()

    # -- 读取 --------------------------------------------------------------

    def load(self, run_id: str, include_events: bool = True) -> RunView:
        with closing(self._connect()) as conn:
            row = self._row(conn, run_id)
            if row is None:
                raise RunNotFoundError(f"运行不存在: {run_id}")
            steps = [
                StepView(
                    name=r["name"], seq=r["seq"], state=r["state"],
                    handler_ref=r["handler_ref"],
                    compensation_ref=r["compensation_ref"],
                    resolver_ref=r["resolver_ref"],
                    params=loads(r["params"]), result=loads(r["result"]),
                    error=loads(r["error"]), op_id=r["op_id"],
                    attempt=r["attempt"],
                )
                for r in conn.execute(
                    "SELECT * FROM steps WHERE run_id = ? ORDER BY seq",
                    (run_id,),
                )
            ]
            comps = [
                CompensationView(
                    step_name=r["step_name"], seq=r["seq"], state=r["state"],
                    compensation_ref=r["compensation_ref"], op_id=r["op_id"],
                    result=loads(r["result"]), error=loads(r["error"]),
                    attempt=r["attempt"],
                )
                for r in conn.execute(
                    "SELECT * FROM compensations WHERE run_id = ?"
                    " ORDER BY seq DESC",
                    (run_id,),
                )
            ]
            events = []
            if include_events:
                events = [
                    EventView(seq=r["seq"], at=r["at"], kind=r["kind"],
                              step_name=r["step_name"], payload=loads(r["payload"]))
                    for r in conn.execute(
                        "SELECT * FROM events WHERE run_id = ? ORDER BY seq",
                        (run_id,),
                    )
                ]
        return RunView(
            run_id=row["run_id"], flow_name=row["flow_name"],
            flow_version=row["flow_version"], state=row["state"],
            is_blocked=row["blocked_reason"] is not None,
            blocked_reason=row["blocked_reason"],
            cancel_requested=bool(row["cancel_requested"]),
            result=loads(row["result"]), failure=loads(row["failure"]),
            run_params=loads(row["params"]),
            created_at=row["created_at"], updated_at=row["updated_at"],
            lease_owner=row["lease_owner"], lease_epoch=row["lease_epoch"],
            lease_expires_at=row["lease_expires_at"],
            steps=tuple(steps), compensations=tuple(comps),
            events=tuple(events),
        )

    def list_runs(self, limit: int = 200) -> list[RunView]:
        with closing(self._connect()) as conn:
            ids = [
                r["run_id"]
                for r in conn.execute(
                    "SELECT run_id FROM runs ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                )
            ]
        return [self.load(rid, include_events=False) for rid in ids]

    def active_run_ids(self) -> list[str]:
        """重启后恢复用：所有非终态、且未被有效租约占用的运行。"""

        now = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        with closing(self._connect()) as conn:
            return [
                r["run_id"]
                for r in conn.execute(
                    "SELECT run_id FROM runs WHERE state IN ('running',"
                    " 'compensating', 'incompatible') AND (lease_owner IS NULL"
                    " OR lease_expires_at <= ?) ORDER BY created_at",
                    (now,),
                )
            ]
