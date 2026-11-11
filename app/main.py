"""敏感证据一次性交接服务。

设计要点见模块内各路由注释；总体安全模型：
- 链接令牌、下载会话秘密均为 32 字节随机数，数据库只存 SHA-256 哈希；
- 打开链接（状态查询）不消耗授权，点击“领取”才在 SQLite 写事务内绑定会话；
- 领取与到期判断全部走共享 SQLite（WAL + BEGIN IMMEDIATE），两个 Web 进程行为一致；
- 上传/下载全程分块流式处理，不做全量内存缓冲。
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import database as db
from . import storage
from .config import (CHUNK_SIZE, COOKIE_NAME, GRANT_TTL_SECONDS,
                     LEASE_TTL_SECONDS, LINK_TTL_SECONDS, MAX_UPLOAD_BYTES)
from .multipart_stream import (StreamingMultipart, UploadError,
                               UploadTooLarge)
from .security import hash_token, new_token, safe_filename

logger = logging.getLogger("handoff")
BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="敏感证据交接", docs_url=None, redoc_url=None, openapi_url=None)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


# ---------------------------------------------------------------------------
# 生命周期：建表、清理遗留临时文件、后台到期回收线程
# ---------------------------------------------------------------------------

_reaper_stop = threading.Event()


def _reaper_loop() -> None:
    """每 15 秒扫描到期授权；可安全删除的交接记录对应文件立即删除。"""
    while not _reaper_stop.wait(15):
        conn = None
        try:
            conn = db.connect()
            expired = db.purge_expired(conn)
        except Exception:  # pragma: no cover - 守护线程容错
            logger.exception("到期清理失败，下轮重试")
            continue
        finally:
            if conn is not None:
                conn.close()
        for row in expired:
            storage.safe_remove_storage(row["storage_name"])
            logger.info("已清理到期交接 id=%s 文件=%s", row["id"], row["storage_name"])


def _purge_now() -> None:
    """访问命中到期记录时同步触发一次清理，缩短文件删除延迟。"""
    conn = None
    try:
        conn = db.connect()
        for row in db.purge_expired(conn):
            storage.safe_remove_storage(row["storage_name"])
    except Exception:  # pragma: no cover
        logger.exception("即时清理失败")
    finally:
        if conn is not None:
            conn.close()


@app.on_event("startup")
def _on_startup() -> None:
    db.init_db()
    storage.cleanup_stale_tmp()
    thread = threading.Thread(target=_reaper_loop, name="handoff-reaper", daemon=True)
    thread.start()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _reaper_stop.set()


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def _conn(request: Request):
    if not hasattr(request.state, "conn"):
        request.state.conn = db.connect()
    return request.state.conn


@app.middleware("http")
async def _close_db(request: Request, call_next):
    try:
        return await call_next(request)
    finally:
        conn = getattr(request.state, "conn", None)
        if conn is not None:
            conn.close()


def _now(conn) -> int:
    return int(conn.execute("SELECT unixepoch() AS t").fetchone()["t"])


def _error(status: int, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail=message)


def _content_disposition(filename: str) -> str:
    return f"attachment; filename*=UTF-8''{quote(filename, safe='')}"


# ---------------------------------------------------------------------------
# 页面
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def page_index():
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/send", include_in_schema=False)
def page_send():
    return FileResponse(BASE_DIR / "static" / "send.html")


@app.get("/receive/{token}", include_in_schema=False)
def page_receive(token: str):
    return FileResponse(BASE_DIR / "static" / "receive.html")


@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}


# ---------------------------------------------------------------------------
# 发送方：流式上传，成功后才发布链接
# ---------------------------------------------------------------------------

@app.post("/api/handoffs")
async def create_handoff(request: Request):
    content_type = request.headers.get("content-type", "")
    if not content_type.lower().startswith("multipart/form-data"):
        raise _error(400, "请求必须是 multipart/form-data")
    if "boundary=" not in content_type.lower():
        raise _error(400, "multipart 缺少 boundary")
    boundary = content_type.split("boundary=", 1)[1].strip().strip('"').encode("latin-1")
    if not boundary or len(boundary) > 70:
        raise _error(400, "非法的 multipart boundary")

    parser = StreamingMultipart(
        receive=request.receive, boundary=boundary, max_size=MAX_UPLOAD_BYTES
    )

    tmp_path = storage.new_tmp_path()
    storage_name = storage.new_storage_name()
    original_name = ""
    content_type_value = "application/octet-stream"
    size = 0
    file_seen = False

    try:
        with tmp_path.open("wb") as fh:
            async for part in parser.parts():
                if part.name != "file" or not part.filename:
                    await part.drain()
                    continue
                if file_seen:
                    raise _error(400, "只允许上传一个文件字段")
                file_seen = True
                original_name = safe_filename(part.filename)
                if part.content_type:
                    content_type_value = part.content_type
                async for chunk in part.chunks():
                    fh.write(chunk)
                    size += len(chunk)

        if not file_seen:
            storage.safe_unlink_tmp(tmp_path)
            raise _error(400, "缺少 file 文件字段")
        if size == 0:
            storage.safe_unlink_tmp(tmp_path)
            raise _error(400, "不允许上传空文件")
        if size > MAX_UPLOAD_BYTES:  # 解析器通常已先抛 413，这里双保险
            storage.safe_unlink_tmp(tmp_path)
            raise _error(413, f"文件超过 {MAX_UPLOAD_BYTES} 字节上限")

        storage.finalize(tmp_path, storage_name)
    except UploadTooLarge as exc:
        storage.safe_unlink_tmp(tmp_path)
        raise _error(413, str(exc)) from exc
    except UploadError as exc:
        storage.safe_unlink_tmp(tmp_path)
        raise _error(400, f"multipart 解析失败：{exc}") from exc
    except HTTPException:
        raise
    except Exception:
        storage.safe_unlink_tmp(tmp_path)
        logger.exception("上传处理失败")
        raise _error(500, "上传处理失败") from None

    link_token = new_token()
    conn = _conn(request)
    try:
        with db.transaction(conn):
            handoff_id = db.create_handoff(
                conn,
                link_hash=hash_token(link_token),
                storage_name=storage_name,
                original_name=original_name,
                content_type=content_type_value,
                size_bytes=size,
                link_ttl=LINK_TTL_SECONDS,
            )
    except Exception:
        storage.safe_remove_storage(storage_name)
        raise _error(500, "授权持久化失败，链接未发布") from None

    return {
        "ok": True,
        "handoff_id": handoff_id,
        "token": link_token,
        "url": f"/receive/{link_token}",
        "filename": original_name,
        "size_bytes": size,
        "expires_in": LINK_TTL_SECONDS,
    }


# ---------------------------------------------------------------------------
# 接收方：状态查询（不绑定）、领取（原子绑定）、下载（单段 Range）
# ---------------------------------------------------------------------------

def _load_handoff(conn, token: str):
    row = db.get_by_link(conn, hash_token(token))
    if row is None:
        raise _error(404, "交接链接不存在或已被清理")
    return row


@app.get("/api/handoffs/{token}/status")
def handoff_status(token: str, request: Request):
    """打开接收页时调用：只读取状态，不消耗、不绑定任何授权。"""
    conn = _conn(request)
    row = _load_handoff(conn, token)
    now = _now(conn)

    if row["session_hash"] is None:
        if row["link_expires_at"] <= now:
            _purge_now()
            raise _error(410, "链接已过期，证据文件已清理")
        return {
            "claimed": False,
            "yours": False,
            "filename": row["original_name"],
            "size_bytes": row["size_bytes"],
            "expires_at": row["link_expires_at"],
        }

    cookie = request.cookies.get(COOKIE_NAME)
    yours = bool(cookie) and hash_token(cookie) == row["session_hash"]
    if row["grant_expires_at"] <= now:
        _purge_now()
        raise _error(410, "续传会话已过期，证据文件已清理")
    if not yours:
        # 已被其他浏览器领取：不透露文件细节之外的信息
        raise _error(409, "该证据已被其他浏览器领取，本链接不能再次绑定")

    # 刷新接收页：授权保留并顺延续传窗口
    with db.transaction(conn):
        db.touch_grant(conn, row["id"], GRANT_TTL_SECONDS)
        row = db.get_by_id(conn, row["id"])
    return {
        "claimed": True,
        "yours": True,
        "filename": row["original_name"],
        "size_bytes": row["size_bytes"],
        "content_type": row["content_type"],
        "expires_at": row["grant_expires_at"],
        "expires_in": GRANT_TTL_SECONDS,
    }


@app.post("/api/handoffs/{token}/claim")
def handoff_claim(token: str, request: Request):
    """点击领取：在写事务内把随机 HttpOnly 会话绑定到链接（仅一次）。"""
    conn = _conn(request)
    row = _load_handoff(conn, token)
    now = _now(conn)

    with db.transaction(conn):
        # 事务内重读，保证与并发领取者串行
        row = db.get_by_link(conn, hash_token(token))
        if row is None:
            raise _error(404, "交接链接不存在或已被清理")

        if row["session_hash"] is not None:
            cookie = request.cookies.get(COOKIE_NAME)
            same_browser = bool(cookie) and hash_token(cookie) == row["session_hash"]
            if not same_browser:
                raise _error(409, "该证据已被其他浏览器领取")
            if row["grant_expires_at"] <= now:
                raise _error(410, "续传会话已过期")
            # 原浏览器重试：仍取得同一份授权，并顺延窗口
            db.touch_grant(conn, row["id"], GRANT_TTL_SECONDS)
            row = db.get_by_id(conn, row["id"])
            session_token = cookie
        else:
            if row["link_expires_at"] <= now:
                raise _error(410, "链接已过期")
            session_token = new_token()
            updated = db.claim_handoff(conn, row["id"], hash_token(session_token), GRANT_TTL_SECONDS)
            if updated == 0:  # 极端并发下抢锁失败
                raise _error(409, "该证据已被其他浏览器领取")
            row = db.get_by_id(conn, row["id"])

    response = JSONResponse(
        {
            "ok": True,
            "claimed": True,
            "filename": row["original_name"],
            "size_bytes": row["size_bytes"],
            "expires_in": GRANT_TTL_SECONDS,
        }
    )
    response.set_cookie(
        key=COOKIE_NAME,
        value=session_token,
        max_age=GRANT_TTL_SECONDS,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


def _parse_single_range(range_header: str, size: int) -> tuple[int, int] | None:
    """只接受单段 bytes=start-end（end 可省略）；其余形式一律 416。"""
    if not range_header.startswith("bytes="):
        return None
    spec = range_header[len("bytes="):].strip()
    if "," in spec or not spec:
        return None
    if spec.startswith("-"):  # 后缀形式 bytes=-N：本实现不支持
        return None
    start_str, sep, end_str = spec.partition("-")
    try:
        start = int(start_str)
        end = int(end_str) if end_str else size - 1
    except ValueError:
        return None
    if start < 0 or end < start or start >= size:
        return None
    return start, min(end, size - 1)


@app.get("/api/handoffs/{token}/download")
def handoff_download(token: str, request: Request):
    conn = _conn(request)
    row = _load_handoff(conn, token)
    now = _now(conn)

    if row["session_hash"] is None:
        raise _error(403, "证据尚未领取，无法下载")
    if row["grant_expires_at"] <= now:
        _purge_now()
        raise _error(410, "续传会话已过期，证据文件已清理")
    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie or hash_token(cookie) != row["session_hash"]:
        raise _error(403, "仅领取该浏览器的会话可以下载")

    size = int(row["size_bytes"])
    range_header = request.headers.get("range")
    start, end = 0, size - 1
    partial = False
    if range_header:
        parsed = _parse_single_range(range_header, size)
        if parsed is None:
            return JSONResponse(
                status_code=416,
                content={"detail": "仅支持单段 bytes=start-end Range 请求"},
                headers={"Content-Range": f"bytes */{size}"},
            )
        start, end = parsed
        partial = True

    path = storage.storage_path(row["storage_name"])
    if path is None or not path.is_file():
        raise _error(404, "证据文件缺失或已被清理")

    # 放行后先登记下载租约：清理线程在租约有效期内不会删除该文件，
    # 保证到期前已获准的下载可以完整结束（支持断线后续传）。
    lease_token = new_token()
    with db.transaction(conn):
        db.touch_grant(conn, row["id"], GRANT_TTL_SECONDS)
        db.add_lease(conn, row["id"], lease_token, LEASE_TTL_SECONDS)

    length = end - start + 1
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": _content_disposition(row["original_name"]),
        "Content-Length": str(length),
        "Cache-Control": "private, no-store",
    }

    def file_iterator():
        # 同步生成器：Starlette 会放到线程池迭代，保持 256 KiB 分块、不整文件缓冲
        with path.open("rb") as fh:
            fh.seek(start)
            remaining = length
            while remaining > 0:
                chunk = fh.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    media_type = row["content_type"] or "application/octet-stream"
    if partial:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        return StreamingResponse(
            file_iterator(), status_code=206, headers=headers, media_type=media_type
        )
    return StreamingResponse(
        file_iterator(), status_code=200, headers=headers, media_type=media_type
    )
