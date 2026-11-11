"""端到端正确性测试（仅依赖标准库 + httpx ASGITransport）。

覆盖：
1. multipart 流式解析器：分块切碎（1 字节/块）、边界跨块、大小上限；
2. 上传 -> 状态（不消耗授权）-> 领取原子性（并发只有一个浏览器成功，
   原浏览器重试取得同一授权）-> 会话隔离 -> 单段 Range/206/416；
3. 未领取链接到期与续传会话到期：拒绝新下载并清理文件、不误删其他文件；
4. 两个独立应用实例（模拟两个 Web 进程）共享 SQLite 时行为一致；
5. 哈希入库：库内不存在原始链接/会话秘密。

运行：HANDOFF_DATA_DIR 由本测试自动指向临时目录，python3 tests/test_handoff.py
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 默认数据目录（解析器单测不使用）；每个端到端测试会切换到独立临时目录
TMP_DATA = tempfile.mkdtemp(prefix="handoff-test-")
os.environ.setdefault("HANDOFF_DATA_DIR", TMP_DATA)
os.environ.setdefault("HANDOFF_LINK_TTL", "2")
os.environ.setdefault("HANDOFF_GRANT_TTL", "2")
os.environ.setdefault("HANDOFF_LEASE_TTL", "30")

import httpx  # noqa: E402

from app import config  # noqa: E402
from app import database as db  # noqa: E402
from app import storage  # noqa: E402
from app.config import COOKIE_NAME  # noqa: E402
from app.main import app  # noqa: E402
from app.multipart_stream import (StreamingMultipart, UploadError,  # noqa: E402
                                  UploadTooLarge)
from app.security import hash_token  # noqa: E402


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. multipart 流式解析器单测
# ---------------------------------------------------------------------------

class FakeReceive:
    """把完整请求体按指定大小切碎，模拟 ASGI receive 分块到达。"""

    def __init__(self, body: bytes, chunk: int):
        self._parts = [body[i:i + chunk] for i in range(0, len(body), chunk)]
        self._idx = 0

    async def __call__(self):
        if self._idx < len(self._parts):
            piece = self._parts[self._idx]
            self._idx += 1
            return {"type": "http.request", "body": piece,
                    "more_body": self._idx < len(self._parts)}
        return {"type": "http.request", "body": b"", "more_body": False}


def build_multipart(boundary: str, fields: list[tuple[str, str, bytes]]) -> bytes:
    out = b""
    for name, filename, content in fields:
        out += f"--{boundary}\r\n".encode()
        out += (f'Content-Disposition: form-data; name="{name}"; '
                f'filename="{filename}"\r\n').encode()
        out += b"Content-Type: application/octet-stream\r\n\r\n"
        out += content + b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return out


class MultipartParserTests(unittest.TestCase):

    def _parse(self, body: bytes, boundary: str, chunk: int, max_size=10_000_000):
        parser = StreamingMultipart(
            FakeReceive(body, chunk), boundary.encode(), max_size=max_size
        )

        async def collect():
            results = []
            async for part in parser.parts():
                chunks = []
                async for piece in part.chunks():
                    chunks.append(piece)
                results.append((part.name, part.filename, b"".join(chunks)))
            return results

        return run(collect())

    def test_byte_by_byte_roundtrip(self):
        boundary = "----testboundary123"
        payload = bytes(range(256)) * 13  # 3328 字节，含所有字节值
        body = build_multipart(boundary, [("file", "evidence.bin", payload)])
        for chunk in (1, 3, 7, 64, 4096):
            with self.subTest(chunk=chunk):
                results = self._parse(body, boundary, chunk)
                self.assertEqual(len(results), 1)
                name, filename, content = results[0]
                self.assertEqual(name, "file")
                self.assertEqual(filename, "evidence.bin")
                self.assertEqual(content, payload)

    def test_multiple_parts_and_nonfile_fields(self):
        boundary = "bnd-xyz"
        body = (
            f"--{boundary}\r\n".encode()
            + b'Content-Disposition: form-data; name="note"\r\n\r\nhello\r\n'
            + f"--{boundary}\r\n".encode()
            + b'Content-Disposition: form-data; name="file"; filename="a.dat"\r\n\r\n'
            + b"abcdef\r\n"
            + f"--{boundary}--\r\n".encode()
        )
        results = self._parse(body, boundary, 2)
        self.assertEqual([r[0] for r in results], ["note", "file"])
        self.assertEqual(results[0][2], b"hello")
        self.assertEqual(results[1][2], b"abcdef")

    def test_size_limit(self):
        boundary = "bnd-limit"
        body = build_multipart(boundary, [("file", "big.bin", b"x" * 5000)])

        async def go():
            parser = StreamingMultipart(
                FakeReceive(body, 16), boundary.encode(), max_size=1000
            )
            async for part in parser.parts():
                async for _ in part.chunks():
                    pass

        with self.assertRaises(UploadTooLarge):
            run(go())

    def test_malformed_body(self):
        async def go():
            parser = StreamingMultipart(
                FakeReceive(b"this is not multipart at all", 8), b"nope", 100
            )
            async for _ in parser.parts():
                pass

        with self.assertRaises(UploadError):
            run(go())

    def test_drain_skips_remaining_part(self):
        boundary = "bnd-drain"
        body = (
            f"--{boundary}\r\n".encode()
            + b'Content-Disposition: form-data; name="file"; filename="a"\r\n\r\n'
            + b"stream-data" * 50 + b"\r\n"
            + f"--{boundary}\r\n".encode()
            + b'Content-Disposition: form-data; name="file"; filename="b"\r\n\r\n'
            + b"second" + b"\r\n"
            + f"--{boundary}--\r\n".encode()
        )

        async def go():
            parser = StreamingMultipart(FakeReceive(body, 5), boundary.encode(), 10_000_000)
            seen = []
            async for part in parser.parts():
                if part.filename == "a":
                    await part.drain()  # 不消费主体，直接排空
                else:
                    async for piece in part.chunks():
                        seen.append(piece)
            return b"".join(seen)

        self.assertEqual(run(go()), b"second")


# ---------------------------------------------------------------------------
# 2. 端到端交接流程
# ---------------------------------------------------------------------------

def multipart_body(filename: str, content: bytes):
    boundary = "handofftestboundary"
    body = build_multipart(boundary, [("file", filename, content)])
    return boundary, body


class HandoffE2ETests(unittest.TestCase):
    def setUp(self):
        # 每个测试独立数据目录 / 独立 SQLite / 独立文件存储，互不污染
        self._data_dir = Path(tempfile.mkdtemp(prefix="handoff-case-"))
        config.DATA_DIR = self._data_dir
        config.DB_PATH = self._data_dir / "handoff.sqlite3"
        config.FILES_DIR = self._data_dir / "files"
        config.TMP_DIR = self._data_dir / "tmp"
        # db/storage 模块通过 config 动态读取路径，重新建表即可
        db.init_db()
        storage.cleanup_stale_tmp()

    @property
    def files_dir(self) -> Path:
        return config.FILES_DIR

    @classmethod
    def setUpClass(cls):
        pass

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://testserver")

    async def _upload(self, client, filename, content):
        boundary, body = multipart_body(filename, content)
        resp = await client.post(
            "/api/handoffs",
            content=body,
            headers={"content-type": f"multipart/form-data; boundary={boundary}"},
        )
        return resp

    def test_full_happy_path_and_range(self):
        async def scenario():
            async with self._client() as browser_a, self._client() as browser_b:
                payload = bytes((i * 37) % 256 for i in range(4096))
                resp = await self._upload(browser_a, "证据-01.bin", payload)
                self.assertEqual(resp.status_code, 200, resp.text)
                data = resp.json()
                token = data["token"]
                url = f"/api/handoffs/{token}"
                self.assertEqual(data["size_bytes"], len(payload))
                self.assertEqual(data["expires_in"], 2)

                # 状态查询不消耗授权：查两次仍是未领取
                for _ in range(2):
                    r = await browser_a.get(f"{url}/status")
                    self.assertEqual(r.status_code, 200)
                    self.assertFalse(r.json()["claimed"])

                # 并发领取：只有一个浏览器成功
                results = await asyncio.gather(
                    browser_a.post(f"{url}/claim"),
                    browser_b.post(f"{url}/claim"),
                )
                codes = sorted(r.status_code for r in results)
                self.assertEqual(codes, [200, 409])
                winner = browser_a if results[0].status_code == 200 else browser_b
                loser = browser_b if winner is browser_a else browser_a

                # 失败方既不能下载，也不能通过 status 看到文件
                self.assertEqual((await loser.get(f"{url}/download")).status_code, 403)
                self.assertEqual((await loser.get(f"{url}/status")).status_code, 409)

                # 原浏览器重试领取：仍取得同一授权（幂等，同一 Cookie）
                cookie_before = winner.cookies.get(COOKIE_NAME)
                retry = await winner.post(f"{url}/claim")
                self.assertEqual(retry.status_code, 200)
                self.assertEqual(winner.cookies.get(COOKIE_NAME), cookie_before)

                # 刷新接收页保留授权
                status = await winner.get(f"{url}/status")
                self.assertEqual(status.status_code, 200)
                self.assertTrue(status.json()["claimed"] and status.json()["yours"])

                # 全量下载
                full = await winner.get(f"{url}/download")
                self.assertEqual(full.status_code, 200)
                self.assertEqual(full.headers["accept-ranges"], "bytes")
                self.assertEqual(full.content, payload)
                self.assertEqual(int(full.headers["content-length"]), len(payload))

                # 单段 Range：206 + Content-Range
                rng = await winner.get(f"{url}/download",
                                       headers={"Range": "bytes=100-199"})
                self.assertEqual(rng.status_code, 206)
                self.assertEqual(rng.content, payload[100:200])
                self.assertEqual(rng.headers["content-range"],
                                 f"bytes 100-199/{len(payload)}")
                self.assertEqual(int(rng.headers["content-length"]), 100)

                # 开区间 Range
                rng2 = await winner.get(f"{url}/download",
                                        headers={"Range": f"bytes={len(payload)-10}-"})
                self.assertEqual(rng2.status_code, 206)
                self.assertEqual(rng2.content, payload[-10:])

                # 多段 / 后缀 / 非法 Range 一律 416
                for bad in ("bytes=0-9,20-29", "bytes=-100", "bytes=999999-"):
                    bad_resp = await winner.get(f"{url}/download",
                                                headers={"Range": bad})
                    self.assertEqual(bad_resp.status_code, 416, bad)
                    self.assertTrue(bad_resp.headers["content-range"].endswith(
                        f"/{len(payload)}"))
                return token

        return run(scenario())

    def test_concurrent_claim_exactly_one_winner_many_clients(self):
        async def scenario():
            async with self._client() as uploader:
                payload = b"concurrent-evidence" * 100
                resp = await self._upload(uploader, "c.bin", payload)
                token = resp.json()["token"]
                url = f"/api/handoffs/{token}/claim"

            clients = [self._client() for _ in range(8)]
            for c in clients:
                await c.__aenter__()
            try:
                results = await asyncio.gather(
                    *[c.post(url) for c in clients],
                    return_exceptions=True,
                )
                wins = [r for r in results if getattr(r, "status_code", None) == 200]
                self.assertEqual(len(wins), 1, "并发领取必须恰好一个成功")
            finally:
                for c in clients:
                    await c.__aexit__(None, None, None)

        run(scenario())

    def test_unclaimed_link_expiry_blocks_and_cleans_file(self):
        async def scenario():
            async with self._client() as client:
                resp = await self._upload(client, "exp1.bin", b"will-expire")
                token = resp.json()["token"]
                storage_name = self._storage_name(token)
                self.assertTrue((self.files_dir / storage_name).is_file())

                await asyncio.sleep(2.2)  # 等链接 TTL 到期

                r_status = await client.get(f"/api/handoffs/{token}/status")
                self.assertIn(r_status.status_code, (410, 404))
                r_claim = await client.post(f"/api/handoffs/{token}/claim")
                self.assertIn(r_claim.status_code, (410, 404))
                r_dl = await client.get(f"/api/handoffs/{token}/download")
                self.assertIn(r_dl.status_code, (403, 404, 410))

                # 触发清理（测试环境无后台线程）：先删库行，再按返回清单删文件
                conn = db.connect()
                try:
                    removed = db.purge_expired(conn)
                finally:
                    conn.close()
                for row in removed:
                    storage.safe_remove_storage(row["storage_name"])
                self.assertFalse((self.files_dir / storage_name).exists(),
                                 "未领取链接到期后文件必须被清理")

        run(scenario())

    def test_grant_expiry_blocks_new_download_and_cleans(self):
        async def scenario():
            async with self._client() as winner, self._client() as other:
                resp = await self._upload(winner, "exp2.bin", b"grant-expire")
                token = resp.json()["token"]
                url = f"/api/handoffs/{token}"
                storage_name = self._storage_name(token)
                self.assertEqual((await winner.post(f"{url}/claim")).status_code, 200)
                self.assertEqual((await winner.get(f"{url}/download")).status_code, 200)

                await asyncio.sleep(2.2)  # 续传会话到期

                # 原浏览器的新下载也被拒绝
                r = await winner.get(f"{url}/download")
                self.assertIn(r.status_code, (410, 404))
                self.assertEqual((await other.post(f"{url}/claim")).status_code, 409)

                # 已完成下载的租约到期后（测试里直接清掉租约），
                # 模拟后台 reaper 的完整动作：删库行 + 按清单删文件
                conn = db.connect()
                try:
                    conn.execute("DELETE FROM download_leases WHERE handoff_id = "
                                 "(SELECT id FROM handoffs WHERE link_hash = ?)",
                                 (hash_token(token),))
                    removed = db.purge_expired(conn)
                finally:
                    conn.close()
                self.assertTrue(any(row["storage_name"] == storage_name for row in removed))
                for row in removed:
                    storage.safe_remove_storage(row["storage_name"])
                self.assertFalse((self.files_dir / storage_name).exists())

        run(scenario())

    def test_inflight_download_protected_by_lease(self):
        """到期前已获准的下载：登记租约后，清理不能删掉正在传的文件。"""
        async def scenario():
            async with self._client() as client:
                payload = b"z" * 3000
                resp = await self._upload(client, "lease.bin", payload)
                token = resp.json()["token"]
                url = f"/api/handoffs/{token}"
                storage_name = self._storage_name(token)
                await client.post(f"{url}/claim")

                # 直接发下载请求并只读头部：服务端已登记租约
                req = client.build_request("GET", f"{url}/download")
                sent = await client.send(req, stream=True)
                self.assertEqual(sent.status_code, 200)
                try:
                    # 强制授权到期
                    conn = db.connect()
                    try:
                        conn.execute(
                            "UPDATE handoffs SET grant_expires_at = unixepoch() - 1 "
                            "WHERE link_hash = ?", (hash_token(token),))
                        removed = db.purge_expired(conn)
                    finally:
                        conn.close()
                    self.assertEqual(removed, [], "存在有效下载租约时不得清理文件")
                    self.assertTrue((self.files_dir / storage_name).is_file())
                    # 流仍可完整读完
                    self.assertEqual(await sent.aread(), payload)
                finally:
                    await sent.aclose()

        run(scenario())

    def test_cleanup_never_touches_other_files(self):
        async def scenario():
            async with self._client() as c:
                keep = (await self._upload(c, "keep.bin", b"keep-me"))
                gone = (await self._upload(c, "gone.bin", b"gone"))
                keep_name = self._storage_name(keep.json()["token"])
                gone_name = self._storage_name(gone.json()["token"])

                conn = db.connect()
                try:
                    conn.execute(
                        "UPDATE handoffs SET link_expires_at = unixepoch() - 1 "
                        "WHERE link_hash = ?", (hash_token(gone.json()["token"]),))
                    removed = db.purge_expired(conn)
                finally:
                    conn.close()
                removed_names = {row["storage_name"] for row in removed}
                self.assertEqual(removed_names, {gone_name})
                # 应用层按清单执行物理删除
                for row in removed:
                    storage.safe_remove_storage(row["storage_name"])
                self.assertTrue((self.files_dir / keep_name).is_file(),
                                "清理绝不能误删其他交接文件")
                self.assertFalse((self.files_dir / gone_name).exists())

        run(scenario())

    def test_upload_rejections_publish_no_link(self):
        async def scenario():
            async with self._client() as client:
                # 空文件
                r1 = await self._upload(client, "empty.bin", b"")
                self.assertEqual(r1.status_code, 400)

                # 超 16 MiB
                big = b"a" * (16 * 1024 * 1024 + 1)
                boundary, body = multipart_body("big.bin", big)
                r2 = await client.post(
                    "/api/handoffs", content=body,
                    headers={"content-type": f"multipart/form-data; boundary={boundary}"},
                )
                self.assertEqual(r2.status_code, 413)

                # 错误 content-type
                r3 = await client.post("/api/handoffs", content=b"x",
                                       headers={"content-type": "text/plain"})
                self.assertEqual(r3.status_code, 400)

                conn = db.connect()
                try:
                    count = conn.execute("SELECT COUNT(*) AS c FROM handoffs").fetchone()["c"]
                finally:
                    conn.close()
                self.assertEqual(count, 0, "失败上传不得产生任何交接记录")

                # 临时目录不得残留
                leftovers = list(config.TMP_DIR.glob("*.part"))
                self.assertEqual(leftovers, [])

        run(scenario())

    def test_only_hashes_persisted(self):
        async def scenario():
            async with self._client() as client:
                resp = await self._upload(client, "secret.bin", b"hash-check")
                token = resp.json()["token"]
                claim = await client.post(f"/api/handoffs/{token}/claim")
                self.assertEqual(claim.status_code, 200)
                session = client.cookies.get(COOKIE_NAME)

            raw_conn = sqlite3.connect(str(config.DB_PATH))
            try:
                rows = raw_conn.execute(
                    "SELECT link_hash, session_hash FROM handoffs"
                ).fetchall()
            finally:
                raw_conn.close()
            self.assertTrue(rows)
            for link_hash, session_hash in rows:
                self.assertEqual(link_hash, hash_token(token))
                self.assertEqual(session_hash, hash_token(session))
                self.assertEqual(len(link_hash), 64)
                self.assertEqual(len(session_hash), 64)

        run(scenario())

    def test_two_app_instances_share_state(self):
        """模拟两个 Web 进程：独立 ASGI app/连接，共用同一 SQLite 数据卷。"""
        # 重新导入一个独立 app 模块实例
        import importlib

        import app.main as main_mod
        app2 = importlib.reload(main_mod).app

        async def scenario():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://p1"
            ) as p1, httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app2), base_url="http://p2"
            ) as p2:
                resp = await self._upload(p1, "shared.bin", b"cross-process")
                token = resp.json()["token"]
                url = f"/api/handoffs/{token}"

                # 进程 2 也能看到链接且未领取
                self.assertFalse((await p2.get(f"{url}/status")).json()["claimed"])

                # 进程 1 领取
                self.assertEqual((await p1.post(f"{url}/claim")).status_code, 200)

                # 进程 2 上同一浏览器会话（带同一 Cookie）可见授权；
                # 无 Cookie 的独立浏览器在进程 2 被拒
                self.assertEqual((await p1.get(f"{url}/status")).status_code, 200)
                bare = httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app2), base_url="http://p2b"
                )
                async with bare:
                    self.assertEqual((await bare.post(f"{url}/claim")).status_code, 409)
                    self.assertEqual((await bare.get(f"{url}/download")).status_code, 403)

                # 带同一会话 Cookie 跨进程下载成功且字节一致
                p2.cookies.set(COOKIE_NAME, p1.cookies.get(COOKIE_NAME))
                dl = await p2.get(f"{url}/download")
                self.assertEqual(dl.status_code, 200)
                self.assertEqual(dl.content, b"cross-process")

        run(scenario())

    # -- helpers ---------------------------------------------------------

    def _storage_name(self, token: str) -> str:
        conn = db.connect()
        try:
            row = conn.execute(
                "SELECT storage_name FROM handoffs WHERE link_hash = ?",
                (hash_token(token),),
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row)
        return row["storage_name"]


if __name__ == "__main__":
    unittest.main(verbosity=2)
