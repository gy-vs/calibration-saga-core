"""SQLite 持久层。

设计约束
--------

1. 所有状态机推进（领取下一步 / 完成一次尝试 / 进入撤回）都是*短*写事务，
   事务期间不执行任何对控制器的调用。因此两个进程不可能在同一个运行里
   同时领取到新的进展动作。
2. 对控制器的外部调用**之前**必须先落一条 ``started`` 尝试记录。进程在
   调用前后任何时刻被杀，重开时都能在 ``run_attempts`` 里看到这次操作身份。
3. 写事务带 fencing 校验（``lock_owner`` + ``lock_epoch``）。旧进程即使
   还活着，租约被接管后它的任何提交都会被数据库拒绝。
4. JSON 值与 SQL NULL 用哨兵区分，空结果 ``{}``/``null`` 都能被确认。
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
import threading
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

from . import models as m
from .errors import (
    InvalidState,
    LibraryProgrammingError,
    RunLockLost,
    UnknownRun,
)

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS flow_runs (
    run_id          TEXT PRIMARY KEY,
    flow_name       TEXT NOT NULL,
    flow_version    INTEGER NOT NULL,
    flow_title      TEXT NOT NULL DEFAULT '',
    definition      TEXT NOT NULL,           -- 开始时固化的完整流程 JSON
    input           TEXT,                    -- 运行入参（JSON 哨兵包裹）
    status          TEXT NOT NULL,
    error           TEXT,
    blocked_reason  TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    lock_owner      TEXT,
    lock_epoch      INTEGER NOT NULL DEFAULT 0,
    locked_until    TEXT,                    -- ISO8601 UTC
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_status ON flow_runs(status);

CREATE TABLE IF NOT EXISTS flow_steps (
    run_id            TEXT NOT NULL REFERENCES flow_runs(run_id),
    seq               INTEGER NOT NULL,
    key               TEXT NOT NULL,
    title             TEXT NOT NULL DEFAULT '',
    handler           TEXT NOT NULL,
    params            TEXT,
    compensator       TEXT,
    compensate_when   TEXT,
    execute_status    TEXT NOT NULL,
    result            TEXT,
    error             TEXT,
    compensate_status TEXT NOT NULL,
    compensate_error  TEXT,
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_steps_key ON flow_steps(run_id, key);

CREATE TABLE IF NOT EXISTS run_attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL REFERENCES flow_runs(run_id),
    step_seq    INTEGER NOT NULL,
    step_key    TEXT NOT NULL,
    kind        TEXT NOT NULL,              -- execute / compensate
    attempt_no  INTEGER NOT NULL,
    op_id       TEXT NOT NULL,
    status      TEXT NOT NULL,              -- started / succeeded / rejected / inconclusive
    result      TEXT,
    error       TEXT,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (run_id, step_seq, kind, attempt_no)
);
CREATE INDEX IF NOT EXISTS idx_attempts_op ON run_attempts(run_id, op_id);

CREATE TABLE IF NOT EXISTS run_facts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL REFERENCES flow_runs(run_id),
    at         TEXT NOT NULL,
    kind       TEXT NOT NULL,
    step_key   TEXT,
    attempt_id INTEGER,
    detail     TEXT
);
CREATE INDEX IF NOT EXISTS idx_facts_run ON run_facts(run_id, id);
"""

_NULL_SENTINEL = {"__null__": True}

# 计划动作类型
ACT_EXECUTE = "execute"
ACT_COMPENSATE = "compensate"
ACT_BLOCKED = "blocked"
ACT_CONTINUE = "continue"   # 事务内状态已推进，没有外部调用，立刻重新计划
ACT_DONE = "done"

_OPEN_ATTEMPT_STATUSES = (m.ATTEMPT_STARTED, m.ATTEMPT_INCONCLUSIVE)


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _wrap_json(value: Any) -> str:
    """把可 JSON 化的值包成可存储文本；Python None 存为显式 null 哨兵。"""

    if value is None:
        return _dumps(_NULL_SENTINEL)
    return _dumps(value)


def _unwrap_json(text: Optional[str]) -> Any:
    if text is None:
        return None
    value = json.loads(text)
    if isinstance(value, dict) and value.get("__null__") is True and len(value) == 1:
        return None
    return value


def ensure_jsonable(value: Any, *, what: str) -> None:
    """提前确认处理函数的返回值可以保存为 JSON（不可保存即视为结果不明）。"""

    try:
        json.dumps(value, default=str)
    except (TypeError, ValueError) as exc:
        raise LibraryProgrammingError(f"{what} 不能保存为 JSON：{exc}") from exc


def _error_payload(exc: BaseException) -> Dict[str, Any]:
    return {"type": type(exc).__name__, "message": str(exc)[:2000]}


def _add_fact_tx(conn, run_id, now, kind, *, step_key=None, attempt_id=None,
                 detail=None) -> None:
    conn.execute(
        "INSERT INTO run_facts(run_id, at, kind, step_key, attempt_id, detail)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, now, kind, step_key, attempt_id,
         _dumps(detail) if detail is not None else None),
    )


def _set_blocked_tx(conn, run_id, now, run_status, reason, *, step_key=None) -> None:
    conn.execute(
        "UPDATE flow_runs SET status=?, blocked_reason=?, updated_at=?"
        " WHERE run_id=?",
        (run_status, _dumps(reason), now, run_id),
    )
    _add_fact_tx(conn, run_id, now, "run_blocked", step_key=step_key, detail=reason)


def _open_attempt_tx(conn, run_id, now, step_row, kind) -> tuple:
    no_row = conn.execute(
        "SELECT COALESCE(MAX(attempt_no), 0) AS n FROM run_attempts"
        " WHERE run_id=? AND step_seq=? AND kind=?",
        (run_id, step_row["seq"], kind),
    ).fetchone()
    attempt_no = int(no_row["n"]) + 1
    op_id = f"{run_id}:{step_row['key']}:{kind}:{attempt_no}"
    cur = conn.execute(
        """
        INSERT INTO run_attempts(run_id, step_seq, step_key, kind, attempt_no,
                                 op_id, status, started_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, step_row["seq"], step_row["key"], kind, attempt_no,
         op_id, m.ATTEMPT_STARTED, now),
    )
    return cur.lastrowid, attempt_no, op_id


def _latest_open_attempt(conn, run_id, step_seq, kind):
    """取该步骤该方向上最近一条仍未关闭（started/inconclusive）的尝试。"""

    placeholders = ",".join("?" for _ in _OPEN_ATTEMPT_STATUSES)
    return conn.execute(
        f"SELECT * FROM run_attempts WHERE run_id=? AND step_seq=? AND kind=?"
        f" AND status IN ({placeholders}) ORDER BY attempt_no DESC LIMIT 1",
        (run_id, step_seq, kind, *_OPEN_ATTEMPT_STATUSES),
    ).fetchone()


def _enter_compensation_tx(conn, run_id, now, step_rows, *, reason: str,
                           trigger_step_key: Optional[str] = None,
                           trigger_attempt_id: Optional[int] = None,
                           predicate: Optional[dict] = None) -> None:
    """在调用事务内把运行切换到撤回：未执行步骤跳过、无补偿步骤标记无需撤回。"""

    for row in step_rows:
        if row["execute_status"] == m.STEP_PENDING:
            conn.execute(
                "UPDATE flow_steps SET execute_status=?, compensate_status=?"
                " WHERE run_id=? AND seq=?",
                (m.STEP_SKIPPED, m.COMP_NOT_NEEDED, run_id, row["seq"]),
            )
            _add_fact_tx(conn, run_id, now, "step_skipped",
                         step_key=row["key"])
        elif (row["execute_status"] == m.STEP_SUCCEEDED
              and row["compensator"] is None):
            conn.execute(
                "UPDATE flow_steps SET compensate_status=? WHERE run_id=? AND seq=?",
                (m.COMP_NOT_NEEDED, run_id, row["seq"]),
            )
    conn.execute(
        "UPDATE flow_runs SET status=?, blocked_reason=NULL, updated_at=?"
        " WHERE run_id=?",
        (m.RUN_COMPENSATING, now, run_id),
    )
    _add_fact_tx(
        conn, run_id, now, "compensation_entered",
        step_key=trigger_step_key, attempt_id=trigger_attempt_id,
        detail={"reason": reason, "predicate": predicate},
    )


def eval_predicate(predicate: Dict[str, Any], result: Any) -> bool:
    ptype = predicate.get("type")
    if ptype == "field_equals":
        field = predicate["field"]
        if not isinstance(result, dict) or field not in result:
            return False
        return result[field] == predicate["equals"]
    raise LibraryProgrammingError(f"不支持的补偿判据：{predicate!r}")


class Database:
    """对一份 SQLite 执行记录的访问。多进程可指向同一个文件。"""

    def __init__(self, path: str, *,
                 now_fn: Optional[Callable[[], _dt.datetime]] = None) -> None:
        self.path = path
        self._now_fn = now_fn or (lambda: _dt.datetime.now(_dt.timezone.utc))
        self._conn = sqlite3.connect(
            path,
            timeout=10.0,
            isolation_level=None,  # autocommit；事务由代码显式 BEGIN/COMMIT
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        # 单条连接可能被驱动线程与心跳线程同时访问；用线程锁串行化所有
        # 短 DB 操作。对控制器的外部调用发生在两次加锁之间，不会被它阻塞。
        self._thread_lock = threading.RLock()
        self._init_schema()

    # ------------------------------------------------------------------
    # 基础
    # ------------------------------------------------------------------

    def _init_schema(self) -> None:
        # WAL 需在事务外设置；脚本内其余语句均为 IF NOT EXISTS。
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        row = self._conn.execute(
            "SELECT value FROM schema_meta WHERE key='version'"
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO schema_meta(key, value) VALUES ('version', ?)",
                (str(SCHEMA_VERSION),),
            )
        elif int(row["value"]) != SCHEMA_VERSION:
            raise RuntimeError(
                f"执行记录 schema 版本为 {row['value']}，库支持 {SCHEMA_VERSION}"
            )

    def close(self) -> None:
        self._conn.close()

    def _now_iso(self) -> str:
        return self._now_fn().isoformat()

    def _tx(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        conn = self._conn
        with self._thread_lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = fn(conn)
                conn.execute("COMMIT")
                return result
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------
    # 运行登记
    # ------------------------------------------------------------------

    def insert_run(
        self,
        *,
        flow_name: str,
        flow_version: int,
        flow_title: str,
        definition: Dict[str, Any],
        run_input: Any,
        steps: List[m.FlowStep],
        run_id: Optional[str] = None,
    ) -> str:
        run_id = run_id or uuid.uuid4().hex
        now = self._now_iso()

        def _work(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO flow_runs(run_id, flow_name, flow_version, flow_title,
                                      definition, input, status,
                                      created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id, flow_name, flow_version, flow_title,
                    _dumps(definition),
                    _wrap_json(run_input),
                    m.RUN_PENDING, now, now,
                ),
            )
            for seq, step in enumerate(steps):
                conn.execute(
                    """
                    INSERT INTO flow_steps(run_id, seq, key, title, handler, params,
                                           compensator, compensate_when,
                                           execute_status, compensate_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id, seq, step.key, step.title,
                        step.handler_name(),
                        _dumps(step.params) if step.params is not None else None,
                        step.compensator_name(),
                        _dumps(step.compensate_when)
                        if step.compensate_when is not None else None,
                        m.STEP_PENDING,
                        m.COMP_NOT_NEEDED if step.compensator_name() is None
                        else m.COMP_PENDING,
                    ),
                )
            _add_fact_tx(conn, run_id, now, "run_registered",
                         detail={"flow": flow_name, "version": flow_version})

        self._tx(_work)
        return run_id

    # ------------------------------------------------------------------
    # 租约（跨进程互斥）+ fencing
    # ------------------------------------------------------------------

    def acquire_lease(
        self,
        run_id: str,
        owner: str,
        *,
        ttl_seconds: float,
        force: bool = False,
    ) -> Optional[int]:
        """取得运行驱动租约，返回 fencing epoch；租约仍有效时返回 None。

        旧租约必须已过期才允许接管（``force`` 只是显式表达"确认旧进程已死"），
        接管会使 epoch 递增，旧 owner 随后的任何提交都被 fencing 拒绝。
        """

        now = self._now_fn()
        now_iso = now.isoformat()
        expires = (now + _dt.timedelta(seconds=ttl_seconds)).isoformat()

        def _work(conn: sqlite3.Connection) -> Optional[int]:
            row = conn.execute(
                "SELECT lock_owner, lock_epoch, locked_until FROM flow_runs"
                " WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise UnknownRun(run_id)

            if row["lock_owner"] is not None:
                locked_until = _dt.datetime.fromisoformat(row["locked_until"])
                if now < locked_until:
                    # force 也不允许抢占未过期租约，避免与存活进程双写。
                    return None
            new_epoch = int(row["lock_epoch"]) + 1
            conn.execute(
                "UPDATE flow_runs SET lock_owner=?, lock_epoch=?, locked_until=?,"
                " updated_at=? WHERE run_id=?",
                (owner, new_epoch, expires, now_iso, run_id),
            )
            _add_fact_tx(conn, run_id, now_iso, "lease_acquired",
                         detail={"owner": owner, "epoch": new_epoch,
                                 "took_over": row["lock_owner"] is not None,
                                 "force": force})
            return new_epoch

        return self._tx(_work)

    def heartbeat(self, run_id: str, owner: str, epoch: int,
                  ttl_seconds: float) -> None:
        expires = (self._now_fn() + _dt.timedelta(seconds=ttl_seconds)).isoformat()

        def _work(conn: sqlite3.Connection) -> bool:
            cur = conn.execute(
                "UPDATE flow_runs SET locked_until=? "
                "WHERE run_id=? AND lock_owner=? AND lock_epoch=?",
                (expires, run_id, owner, epoch),
            )
            return cur.rowcount == 1

        if not self._tx(_work):
            raise RunLockLost(f"运行 {run_id} 的租约已被接管（owner={owner}）")

    def release_lease(self, run_id: str, owner: str, epoch: int) -> None:
        def _work(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE flow_runs SET lock_owner=NULL, locked_until=NULL,"
                " updated_at=? WHERE run_id=? AND lock_owner=? AND lock_epoch=?",
                (self._now_iso(), run_id, owner, epoch),
            )

        self._tx(_work)

    def request_cancel(self, run_id: str) -> None:
        """登记取消请求（不需要持有租约的短事务）。

        只是一个持久标记：是否真的取消、仪器是否回到原参数，仍由持有租约的
        驱动循环按撤回流程确认。取消 Python 协程本身不产生任何外部效果。
        """

        now = self._now_iso()

        def _work(conn: sqlite3.Connection) -> None:
            row = conn.execute(
                "SELECT status, cancel_requested FROM flow_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise UnknownRun(run_id)
            if row["status"] in m.RUN_TERMINAL:
                raise InvalidState(
                    f"运行 {run_id} 已结束（{row['status']}），不能取消")
            if row["cancel_requested"]:
                return
            conn.execute(
                "UPDATE flow_runs SET cancel_requested=1, updated_at=? WHERE run_id=?",
                (now, run_id),
            )
            _add_fact_tx(conn, run_id, now, "cancel_requested")

        self._tx(_work)

    # ------------------------------------------------------------------
    # 推进状态机
    # ------------------------------------------------------------------

    def plan_next_action(
        self,
        run_id: str,
        owner: str,
        epoch: int,
        resolve: Callable[[str], Optional[Callable[..., Awaitable[Any]]]],
    ) -> Dict[str, Any]:
        """在短事务内决定并原子登记下一个动作。

        返回::

            {"action": "execute"|"compensate", "step_seq", "step_key",
             "handler", "op_id", "attempt_id", "attempt_no", "replay"}
            {"action": "blocked", "reason": {...}}
            {"action": "continue"}                  -- 事务内状态已变，立即重新计划
            {"action": "done", "status": 终态}
        """

        now = self._now_iso()
        return self._tx(
            lambda conn: _plan_tx(conn, run_id, owner, epoch, resolve, now)
        )

    def finish_attempt(
        self,
        run_id: str,
        owner: str,
        epoch: int,
        *,
        action: Dict[str, Any],
        outcome_status: str,
        result: Any = None,
        error: Optional[Dict[str, Any]] = None,
        predicate: Optional[Dict[str, Any]] = None,
    ) -> None:
        """落一次外部调用结果，并在同一事务内推进步骤 / 运行状态。"""

        now = self._now_iso()
        self._tx(lambda conn: _finish_tx(
            conn, run_id, owner, epoch, action, outcome_status,
            result, error, predicate, now,
        ))

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def list_run_ids(self) -> List[str]:
        with self._thread_lock:
            rows = self._conn.execute(
                "SELECT run_id FROM flow_runs ORDER BY created_at"
            ).fetchall()
            return [r["run_id"] for r in rows]

    def load_definition(self, run_id: str) -> Dict[str, Any]:
        with self._thread_lock:
            row = self._conn.execute(
                "SELECT definition FROM flow_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise UnknownRun(run_id)
            return json.loads(row["definition"])

    def load_step_invocation(self, run_id: str, step_seq: int):
        """驱动一次外部调用所需的步骤静态信息（在同一把线程锁内读取）。"""

        with self._thread_lock:
            row = self._conn.execute(
                "SELECT params, compensate_when FROM flow_steps"
                " WHERE run_id=? AND seq=?",
                (run_id, step_seq),
            ).fetchone()
            if row is None:
                return None, None
            params = json.loads(row["params"]) if row["params"] else None
            predicate = (json.loads(row["compensate_when"])
                         if row["compensate_when"] else None)
            return params, predicate

    def load_invocation_context(self, run_id: str):
        """读取运行入参与所有已确认步骤结果（供处理函数/恢复函数使用）。"""

        with self._thread_lock:
            run = self._conn.execute(
                "SELECT input FROM flow_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            run_input = _unwrap_json(run["input"]) if run is not None else None
            rows = self._conn.execute(
                "SELECT key, result FROM flow_steps WHERE run_id=? "
                "AND execute_status=?",
                (run_id, m.STEP_SUCCEEDED),
            ).fetchall()
            results = {row["key"]: _unwrap_json(row["result"]) for row in rows}
            return run_input, results

    def load_snapshot(self, run_id: str) -> Optional[m.RunSnapshot]:
        with self._thread_lock:
            run = self._conn.execute(
                "SELECT * FROM flow_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if run is None:
                return None

            attempt_rows = self._conn.execute(
                "SELECT * FROM run_attempts WHERE run_id=? ORDER BY id", (run_id,)
            ).fetchall()

        def attempt_snapshot(a: sqlite3.Row) -> m.AttemptSnapshot:
            return m.AttemptSnapshot(
                id=a["id"], step_key=a["step_key"], kind=a["kind"],
                op_id=a["op_id"], status=a["status"],
                result=_unwrap_json(a["result"]),
                error=json.loads(a["error"]) if a["error"] else None,
                started_at=a["started_at"], finished_at=a["finished_at"],
                attempt_no=a["attempt_no"],
            )

        latest_exec: Dict[int, sqlite3.Row] = {}
        latest_comp: Dict[int, sqlite3.Row] = {}
        for a in attempt_rows:
            target = latest_exec if a["kind"] == m.ATTEMPT_KIND_EXEC else latest_comp
            old = target.get(a["step_seq"])
            if old is None or a["attempt_no"] > old["attempt_no"]:
                target[a["step_seq"]] = a

        step_rows = self._conn.execute(
            "SELECT * FROM flow_steps WHERE run_id=? ORDER BY seq", (run_id,)
        ).fetchall()
        steps: List[m.StepSnapshot] = []
        for row in step_rows:
            exec_row = latest_exec.get(row["seq"])
            comp_row = latest_comp.get(row["seq"])
            steps.append(m.StepSnapshot(
                seq=row["seq"], key=row["key"], title=row["title"],
                handler=row["handler"],
                params=json.loads(row["params"]) if row["params"] else None,
                compensator=row["compensator"],
                compensate_when=json.loads(row["compensate_when"])
                if row["compensate_when"] else None,
                execute_status=row["execute_status"],
                execute_attempt=attempt_snapshot(exec_row) if exec_row else None,
                result=_unwrap_json(row["result"]),
                error=json.loads(row["error"]) if row["error"] else None,
                compensate_status=row["compensate_status"],
                compensate_attempt=attempt_snapshot(comp_row) if comp_row else None,
                compensate_error=json.loads(row["compensate_error"])
                if row["compensate_error"] else None,
            ))

        fact_rows = self._conn.execute(
            "SELECT * FROM run_facts WHERE run_id=? ORDER BY id", (run_id,)
        ).fetchall()
        facts = [
            m.FactSnapshot(
                seq=f["id"], at=f["at"], kind=f["kind"],
                step_key=f["step_key"], attempt_id=f["attempt_id"],
                detail=json.loads(f["detail"]) if f["detail"] else None,
            )
            for f in fact_rows
        ]

        return m.RunSnapshot(
            run_id=run["run_id"], flow_name=run["flow_name"],
            flow_version=run["flow_version"], flow_title=run["flow_title"],
            status=run["status"], input=_unwrap_json(run["input"]),
            error=json.loads(run["error"]) if run["error"] else None,
            blocked_reason=json.loads(run["blocked_reason"])
            if run["blocked_reason"] else None,
            cancel_requested=bool(run["cancel_requested"]),
            created_at=run["created_at"], updated_at=run["updated_at"],
            steps=steps, facts=facts,
        )


# ===========================================================================
# 状态机事务（模块级，便于逐事务阅读与测试）
# ===========================================================================

def _fenced_run(conn, run_id, owner, epoch):
    run = conn.execute(
        "SELECT * FROM flow_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    if run is None:
        raise UnknownRun(run_id)
    if run["lock_owner"] != owner or int(run["lock_epoch"]) != epoch:
        raise RunLockLost(
            f"fencing 校验失败：期望 owner={owner!r} epoch={epoch}，"
            f"实际 owner={run['lock_owner']!r} epoch={run['lock_epoch']}"
        )
    return run


def _handler_unavailable_reason(step_row, phase, handler_name, op_id=None):
    action_word = "处理" if phase == "execute" else "恢复"
    return {
        "code": "handler_unavailable",
        "phase": phase,
        "step": step_row["key"],
        "handler": handler_name,
        "op_id": op_id,
        "message": (
            f"步骤 {step_row['key']!r} 的{action_word}函数 {handler_name!r} "
            "在当前库实例中未登记。该运行按其开始时固化的流程定义解释，"
            "不能替换为其它函数继续。"
        ),
    }


def _replay_action(conn, run_id, now, run, step_row, kind, resolve):
    """对 started/inconclusive 的尝试用同一 op_id 再确认；缺函数则阻塞。"""

    if kind == m.ATTEMPT_KIND_EXEC:
        handler_name = step_row["handler"]
        run_status = m.RUN_AWAITING_CONFIRMATION
        action_kind = ACT_EXECUTE
    else:
        handler_name = step_row["compensator"]
        run_status = m.RUN_COMPENSATION_BLOCKED
        action_kind = ACT_COMPENSATE

    attempt = _latest_open_attempt(conn, run_id, step_row["seq"], kind)
    if handler_name is None or resolve(handler_name) is None or attempt is None:
        op_id = attempt["op_id"] if attempt is not None else None
        reason = _handler_unavailable_reason(
            step_row,
            "execute" if kind == m.ATTEMPT_KIND_EXEC else "compensate",
            handler_name, op_id,
        )
        if attempt is None:
            reason = {
                "code": "dangling_step_state",
                "phase": reason["phase"],
                "step": step_row["key"],
                "message": "步骤处于未关闭状态但找不到对应尝试记录。",
            }
        _set_blocked_tx(conn, run_id, now, run_status, reason,
                        step_key=step_row["key"])
        return {"action": ACT_BLOCKED, "reason": reason}

    if run["status"] != run_status:
        conn.execute(
            "UPDATE flow_runs SET status=?, updated_at=? WHERE run_id=?",
            (run_status, now, run_id),
        )
    _add_fact_tx(conn, run_id, now, "attempt_replayed",
                 step_key=step_row["key"], attempt_id=attempt["id"],
                 detail={"op_id": attempt["op_id"], "kind": kind})
    return {
        "action": action_kind,
        "step_seq": step_row["seq"],
        "step_key": step_row["key"],
        "handler": handler_name,
        "op_id": attempt["op_id"],
        "attempt_id": attempt["id"],
        "attempt_no": attempt["attempt_no"],
        "replay": True,
    }


def _plan_tx(conn, run_id, owner, epoch, resolve, now):
    run = _fenced_run(conn, run_id, owner, epoch)
    status = run["status"]
    if status in m.RUN_TERMINAL:
        return {"action": ACT_DONE, "status": status}

    step_rows = conn.execute(
        "SELECT * FROM flow_steps WHERE run_id=? ORDER BY seq", (run_id,)
    ).fetchall()

    if status in (m.RUN_COMPENSATING, m.RUN_COMPENSATION_BLOCKED):
        return _plan_compensate(conn, run_id, now, run, step_rows, resolve)
    return _plan_forward(conn, run_id, now, run, step_rows, resolve,
                         cancel=bool(run["cancel_requested"]))


def _plan_forward(conn, run_id, now, run, step_rows, resolve, *, cancel):
    # 1) 结果不明的既有尝试优先：用原 op_id 再确认（崩溃恢复关键路径）。
    for row in step_rows:
        if row["execute_status"] in (m.STEP_STARTED, m.STEP_INCONCLUSIVE):
            return _replay_action(
                conn, run_id, now, run, row, m.ATTEMPT_KIND_EXEC, resolve)

    # 2) 取消请求在事务内转入撤回，保证不与新的前向写入交织。
    if cancel:
        _enter_compensation_tx(conn, run_id, now, step_rows,
                               reason="cancel_requested")
        return {"action": ACT_CONTINUE}

    # 3) 第一个尚未完成的前向步骤。
    for row in step_rows:
        estatus = row["execute_status"]
        if estatus == m.STEP_SUCCEEDED:
            continue
        if estatus == m.STEP_PENDING:
            handler_name = row["handler"]
            if resolve(handler_name) is None:
                reason = _handler_unavailable_reason(row, "execute", handler_name)
                _set_blocked_tx(conn, run_id, now,
                                m.RUN_AWAITING_CONFIRMATION, reason,
                                step_key=row["key"])
                return {"action": ACT_BLOCKED, "reason": reason}
            return _begin_execute(conn, run_id, now, row)
        # rejected/skipped 不应出现在前向扫描中。
        reason = {"code": "unexpected_step_status", "step": row["key"],
                  "execute_status": estatus}
        _set_blocked_tx(conn, run_id, now, m.RUN_AWAITING_CONFIRMATION, reason,
                        step_key=row["key"])
        return {"action": ACT_BLOCKED, "reason": reason}

    # 4) 全部成功。
    conn.execute(
        "UPDATE flow_runs SET status=?, blocked_reason=NULL, error=NULL,"
        " updated_at=? WHERE run_id=?",
        (m.RUN_SUCCEEDED, now, run_id),
    )
    _add_fact_tx(conn, run_id, now, "run_succeeded")
    return {"action": ACT_DONE, "status": m.RUN_SUCCEEDED}


def _begin_execute(conn, run_id, now, row):
    attempt_id, attempt_no, op_id = _open_attempt_tx(
        conn, run_id, now, row, m.ATTEMPT_KIND_EXEC)
    conn.execute(
        "UPDATE flow_steps SET execute_status=?, error=NULL"
        " WHERE run_id=? AND seq=?",
        (m.STEP_STARTED, run_id, row["seq"]),
    )
    conn.execute(
        "UPDATE flow_runs SET status=?, blocked_reason=NULL, updated_at=?"
        " WHERE run_id=?",
        (m.RUN_RUNNING, now, run_id),
    )
    _add_fact_tx(conn, run_id, now, "step_started", step_key=row["key"],
                 attempt_id=attempt_id, detail={"op_id": op_id})
    return {
        "action": ACT_EXECUTE,
        "step_seq": row["seq"],
        "step_key": row["key"],
        "handler": row["handler"],
        "op_id": op_id,
        "attempt_id": attempt_id,
        "attempt_no": attempt_no,
        "replay": False,
    }


def _plan_compensate(conn, run_id, now, run, step_rows, resolve):
    # 1) 结果不明的撤回 / 执行尝试优先确认。
    for row in reversed(step_rows):
        if row["compensate_status"] in (m.COMP_STARTED, m.COMP_INCONCLUSIVE):
            return _replay_action(
                conn, run_id, now, run, row, m.ATTEMPT_KIND_COMP, resolve)
    for row in reversed(step_rows):
        if row["execute_status"] in (m.STEP_STARTED, m.STEP_INCONCLUSIVE):
            return _replay_action(
                conn, run_id, now, run, row, m.ATTEMPT_KIND_EXEC, resolve)

    # 2) 逆依赖顺序撤回。
    for row in reversed(step_rows):
        cstatus = row["compensate_status"]
        if cstatus in (m.COMP_SUCCEEDED, m.COMP_NOT_NEEDED):
            continue
        if cstatus == m.COMP_PENDING:
            if row["execute_status"] != m.STEP_SUCCEEDED:
                # 从未确认生效过的步骤无需撤回。
                conn.execute(
                    "UPDATE flow_steps SET compensate_status=? WHERE run_id=? AND seq=?",
                    (m.COMP_NOT_NEEDED, run_id, row["seq"]),
                )
                continue
            comp_name = row["compensator"]
            if comp_name is None or resolve(comp_name) is None:
                reason = _handler_unavailable_reason(row, "compensate", comp_name)
                _set_blocked_tx(conn, run_id, now,
                                m.RUN_COMPENSATION_BLOCKED, reason,
                                step_key=row["key"])
                return {"action": ACT_BLOCKED, "reason": reason}
            return _begin_compensate(conn, run_id, now, row)
        reason = {"code": "unexpected_compensate_status", "step": row["key"],
                  "compensate_status": cstatus}
        _set_blocked_tx(conn, run_id, now, m.RUN_COMPENSATION_BLOCKED, reason,
                        step_key=row["key"])
        return {"action": ACT_BLOCKED, "reason": reason}

    # 3) 撤回完成：取消且没有任何外部效果 -> cancelled，否则 compensated。
    any_effect = any(r["execute_status"] == m.STEP_SUCCEEDED for r in step_rows)
    if run["cancel_requested"] and not any_effect:
        final_status, fact_kind = m.RUN_CANCELLED, "run_cancelled"
    else:
        final_status, fact_kind = m.RUN_COMPENSATED, "run_compensated"
    conn.execute(
        "UPDATE flow_runs SET status=?, blocked_reason=NULL, updated_at=?"
        " WHERE run_id=?",
        (final_status, now, run_id),
    )
    _add_fact_tx(conn, run_id, now, fact_kind)
    return {"action": ACT_DONE, "status": final_status}


def _begin_compensate(conn, run_id, now, row):
    attempt_id, attempt_no, op_id = _open_attempt_tx(
        conn, run_id, now, row, m.ATTEMPT_KIND_COMP)
    conn.execute(
        "UPDATE flow_steps SET compensate_status=? WHERE run_id=? AND seq=?",
        (m.COMP_STARTED, run_id, row["seq"]),
    )
    conn.execute(
        "UPDATE flow_runs SET status=?, updated_at=? WHERE run_id=?",
        (m.RUN_COMPENSATING, now, run_id),
    )
    _add_fact_tx(conn, run_id, now, "compensate_started",
                 step_key=row["key"], attempt_id=attempt_id,
                 detail={"op_id": op_id})
    return {
        "action": ACT_COMPENSATE,
        "step_seq": row["seq"],
        "step_key": row["key"],
        "handler": row["compensator"],
        "op_id": op_id,
        "attempt_id": attempt_id,
        "attempt_no": attempt_no,
        "replay": False,
    }


# ===========================================================================
# 完成尝试事务
# ===========================================================================

def _finish_tx(conn, run_id, owner, epoch, action, outcome_status, result,
               error, predicate, now):
    run = _fenced_run(conn, run_id, owner, epoch)
    attempt_id = action["attempt_id"]
    is_comp = action["action"] == ACT_COMPENSATE
    kind = m.ATTEMPT_KIND_COMP if is_comp else m.ATTEMPT_KIND_EXEC

    row = conn.execute(
        "SELECT * FROM flow_steps WHERE run_id=? AND seq=?",
        (run_id, action["step_seq"]),
    ).fetchone()

    if outcome_status == m.ATTEMPT_SUCCEEDED:
        conn.execute(
            "UPDATE run_attempts SET status=?, result=?, error=NULL, finished_at=?"
            " WHERE id=? AND run_id=?",
            (m.ATTEMPT_SUCCEEDED, _wrap_json(result), now, attempt_id, run_id),
        )
    else:
        conn.execute(
            "UPDATE run_attempts SET status=?, result=NULL, error=?, finished_at=?"
            " WHERE id=? AND run_id=?",
            (outcome_status,
             _dumps(error) if error is not None else None,
             now, attempt_id, run_id),
        )

    if not is_comp:
        _finish_execute(conn, run_id, now, run, row, outcome_status, result,
                        error, predicate, attempt_id, action)
    else:
        _finish_compensate(conn, run_id, now, row, outcome_status, error,
                           attempt_id, action)

    conn.execute(
        "UPDATE flow_runs SET updated_at=? WHERE run_id=?", (now, run_id))


def _finish_execute(conn, run_id, now, run, row, outcome_status, result,
                    error, predicate, attempt_id, action):
    if outcome_status == m.ATTEMPT_SUCCEEDED:
        conn.execute(
            "UPDATE flow_steps SET execute_status=?, result=?, error=NULL"
            " WHERE run_id=? AND seq=?",
            (m.STEP_SUCCEEDED, _wrap_json(result), run_id, row["seq"]),
        )
        _add_fact_tx(conn, run_id, now, "step_succeeded",
                     step_key=row["key"], attempt_id=attempt_id,
                     detail={"op_id": action["op_id"]})
        # 验证判据与成功在同一事务内生效：命中即整体转撤回，
        # 尚未执行的步骤不会继续。
        if predicate is not None and eval_predicate(predicate, result):
            step_rows = conn.execute(
                "SELECT * FROM flow_steps WHERE run_id=? ORDER BY seq", (run_id,),
            ).fetchall()
            _enter_compensation_tx(
                conn, run_id, now, step_rows, reason="predicate",
                trigger_step_key=row["key"],
                trigger_attempt_id=attempt_id, predicate=predicate,
            )
        return

    if outcome_status == m.ATTEMPT_REJECTED:
        # 明确拒绝：确定没有外部效果。本步骤确定失败，运行转撤回。
        conn.execute(
            "UPDATE flow_steps SET execute_status=?, result=NULL, error=?,"
            " compensate_status=? WHERE run_id=? AND seq=?",
            (m.STEP_REJECTED, _dumps(error) if error else None,
             m.COMP_NOT_NEEDED, run_id, row["seq"]),
        )
        _add_fact_tx(conn, run_id, now, "step_rejected",
                     step_key=row["key"], attempt_id=attempt_id, detail=error)
        step_rows = conn.execute(
            "SELECT * FROM flow_steps WHERE run_id=? ORDER BY seq", (run_id,),
        ).fetchall()
        _enter_compensation_tx(conn, run_id, now, step_rows,
                               reason="step_rejected",
                               trigger_step_key=row["key"],
                               trigger_attempt_id=attempt_id)
        return

    # 结果不明：尝试保留事实，步骤/运行阻塞，等待用原 op_id 再确认。
    reason = {
        "code": "inconclusive_execute",
        "step": row["key"],
        "op_id": action["op_id"],
        "error": error,
        "message": (
            "对控制器的操作结果不明，可能已经生效；恢复时必须使用同一 "
            f"op_id={action['op_id']} 查询，不得重新生成操作。"
        ),
    }
    conn.execute(
        "UPDATE flow_steps SET execute_status=?, error=? WHERE run_id=? AND seq=?",
        (m.STEP_INCONCLUSIVE, _dumps(reason), run_id, row["seq"]),
    )
    conn.execute(
        "UPDATE flow_runs SET status=?, blocked_reason=? WHERE run_id=?",
        (m.RUN_AWAITING_CONFIRMATION, _dumps(reason), run_id),
    )
    _add_fact_tx(conn, run_id, now, "step_inconclusive",
                 step_key=row["key"], attempt_id=attempt_id, detail=reason)


def _finish_compensate(conn, run_id, now, row, outcome_status, error,
                       attempt_id, action):
    if outcome_status == m.ATTEMPT_SUCCEEDED:
        conn.execute(
            "UPDATE flow_steps SET compensate_status=?, compensate_error=NULL"
            " WHERE run_id=? AND seq=?",
            (m.COMP_SUCCEEDED, run_id, row["seq"]),
        )
        _add_fact_tx(conn, run_id, now, "compensate_succeeded",
                     step_key=row["key"], attempt_id=attempt_id,
                     detail={"op_id": action["op_id"]})
        return

    if outcome_status == m.ATTEMPT_REJECTED:
        # 撤回被明确拒绝 -> 确定没恢复成；下一轮用新 op_id 再尝试。
        payload = dict(error or {"message": "恢复操作被拒绝"})
        payload["op_id"] = action["op_id"]
        conn.execute(
            "UPDATE flow_steps SET compensate_status=?, compensate_error=?"
            " WHERE run_id=? AND seq=?",
            (m.COMP_PENDING, _dumps(payload), run_id, row["seq"]),
        )
        _add_fact_tx(conn, run_id, now, "compensate_rejected",
                     step_key=row["key"], attempt_id=attempt_id, detail=payload)
        return

    # 撤回结果不明：明确报告"仪器还没恢复到什么状态"。
    reason = {
        "code": "inconclusive_compensate",
        "step": row["key"],
        "op_id": action["op_id"],
        "error": error,
        "message": (
            f"步骤 {row['key']!r} 的恢复操作结果不明，仪器可能仍停留在该步骤"
            "造成的参数上；适配器恢复后用同一 op_id 查询继续。"
        ),
    }
    conn.execute(
        "UPDATE flow_steps SET compensate_status=?, compensate_error=?"
        " WHERE run_id=? AND seq=?",
        (m.COMP_INCONCLUSIVE, _dumps(reason), run_id, row["seq"]),
    )
    conn.execute(
        "UPDATE flow_runs SET status=?, blocked_reason=? WHERE run_id=?",
        (m.RUN_COMPENSATION_BLOCKED, _dumps(reason), run_id),
    )
    _add_fact_tx(conn, run_id, now, "compensate_inconclusive",
                 step_key=row["key"], attempt_id=attempt_id, detail=reason)
