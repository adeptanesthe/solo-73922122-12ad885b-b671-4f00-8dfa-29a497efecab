"""SQLite 持久层。

- WAL + BEGIN IMMEDIATE：两个 Web 进程并发领取同一链接时，写锁串行化，
  保证只有一个浏览器能完成绑定；其余进程读到已领取状态而失败。
- 库内只保存链接令牌哈希与下载会话哈希，不保存任何原始秘密。
- 所有到期判断统一以数据库时钟 unixepoch() 为准，两个进程结论一致。
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import config
from .config import ensure_dirs

SCHEMA = """
CREATE TABLE IF NOT EXISTS handoffs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    link_hash        TEXT NOT NULL UNIQUE,
    session_hash     TEXT,
    storage_name     TEXT NOT NULL,
    original_name    TEXT NOT NULL,
    content_type     TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL,
    created_at       INTEGER NOT NULL,
    link_expires_at  INTEGER NOT NULL,
    grant_expires_at INTEGER,
    claimed_at       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_handoffs_session ON handoffs(session_hash);

CREATE TABLE IF NOT EXISTS download_leases (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    handoff_id  INTEGER NOT NULL REFERENCES handoffs(id),
    token       TEXT NOT NULL UNIQUE,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leases_handoff ON download_leases(handoff_id);
"""


def connect() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(
        f"file:{config.DB_PATH}?immutable=0",
        uri=True,
        timeout=30.0,
        isolation_level=None,  # autocommit；事务边界显式控制
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    conn = connect()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE 立即取写锁；并发领取在此串行化。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def create_handoff(
    conn: sqlite3.Connection,
    *,
    link_hash: str,
    storage_name: str,
    original_name: str,
    content_type: str,
    size_bytes: int,
    link_ttl: int,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO handoffs (link_hash, storage_name, original_name, content_type,
                              size_bytes, created_at, link_expires_at)
        VALUES (?, ?, ?, ?, ?, unixepoch(), unixepoch() + ?)
        """,
        (link_hash, storage_name, original_name, content_type, size_bytes, link_ttl),
    )
    return int(cur.lastrowid)


def get_by_link(conn: sqlite3.Connection, link_hash: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM handoffs WHERE link_hash = ?", (link_hash,)
    ).fetchone()


def get_by_id(conn: sqlite3.Connection, handoff_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM handoffs WHERE id = ?", (handoff_id,)
    ).fetchone()


def claim_handoff(conn: sqlite3.Connection, handoff_id: int, session_hash: str, grant_ttl: int) -> int:
    """在已持有的写事务内绑定下载会话（仅限尚未领取的记录）。返回受影响行数。"""
    cur = conn.execute(
        """
        UPDATE handoffs
           SET session_hash = ?,
               claimed_at = unixepoch(),
               grant_expires_at = unixepoch() + ?
         WHERE id = ? AND session_hash IS NULL
        """,
        (session_hash, grant_ttl, handoff_id),
    )
    return cur.rowcount


def touch_grant(conn: sqlite3.Connection, handoff_id: int, grant_ttl: int) -> None:
    """刷新接收页时顺延续传窗口。"""
    conn.execute(
        "UPDATE handoffs SET grant_expires_at = unixepoch() + ? WHERE id = ?",
        (grant_ttl, handoff_id),
    )


def add_lease(conn: sqlite3.Connection, handoff_id: int, token: str, lease_ttl: int) -> None:
    conn.execute(
        """
        INSERT INTO download_leases (handoff_id, token, expires_at)
        VALUES (?, ?, unixepoch() + ?)
        """,
        (handoff_id, token, lease_ttl),
    )


def active_lease_exists(conn: sqlite3.Connection, handoff_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM download_leases WHERE handoff_id = ? AND expires_at > unixepoch() LIMIT 1",
        (handoff_id,),
    ).fetchone()
    return row is not None


def purge_expired(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """清理到期授权并返回可以安全删除文件的记录。

    可删除条件（同一把写锁内判定，避免与下载流程竞争）：
      1. 未领取且链接到期，或已领取且续传会话到期；并且
      2. 当前没有任何未结束的下载租约。
    其他交接的记录与文件不会被选中，绝不误删。
    """
    removable: list[sqlite3.Row] = []
    with transaction(conn):
        conn.execute("DELETE FROM download_leases WHERE expires_at <= unixepoch()")
        expired = conn.execute(
            """
            SELECT * FROM handoffs
             WHERE (session_hash IS NULL AND link_expires_at <= unixepoch())
                OR (session_hash IS NOT NULL AND grant_expires_at <= unixepoch())
            """
        ).fetchall()
        for row in expired:
            if active_lease_exists(conn, row["id"]):
                # 到期前已获准的下载仍在进行：保留文件，等租约结束后下一轮再删
                continue
            conn.execute("DELETE FROM handoffs WHERE id = ?", (row["id"],))
            removable.append(row)
    return removable
