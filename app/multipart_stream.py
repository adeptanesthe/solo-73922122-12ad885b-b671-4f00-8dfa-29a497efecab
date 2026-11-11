"""有界内存的 multipart/form-data 流式解析器。

不依赖 python-multipart，也不调用会把请求体整体读入内存的 API：
- 每次只从 ASGI receive() 拉取一个网络分片；
- 边界匹配使用滚动窗口（保留不超过一个边界长度的尾部前缀），常驻内存有界；
- 文件部分以生成器逐块吐出，由调用方直接落盘。

RFC 2046 语义：每个 part 的主体在 CRLF + "--边界" 处结束；
结束标记 "--边界--" 后整个 multipart 体结束。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field

MAX_HEADER_BYTES = 16 * 1024  # 单个 part 的头部上限
PREFIX_MAX = 64 * 1024        # 边界前缀窗口上限，超过即判定数据异常


class UploadError(Exception):
    """multipart 解析失败（对应 HTTP 400）。"""


class UploadTooLarge(UploadError):
    """超过上传大小上限（对应 HTTP 413）。"""


@dataclass
class Part:
    name: str
    filename: str | None
    content_type: str | None
    headers: dict[str, str]
    _stream: "StreamingMultipart"

    async def chunks(self) -> AsyncIterator[bytes]:
        async for piece in self._stream.iter_body(self):
            yield piece

    async def drain(self) -> None:
        """消费完本 part 剩余数据，使解析器可以继续定位下一个边界。"""
        async for _ in self.chunks():
            pass


@dataclass
class StreamingMultipart:
    receive: object
    boundary: bytes
    max_size: int
    _buf: bytearray = field(default_factory=bytearray)
    _eof: bool = False
    _current: Part | None = None
    _emitted: int = 0

    def __post_init__(self) -> None:
        self._delimiter = b"--" + self.boundary       # 边界标记本身
        self._separator = b"\r\n" + self._delimiter   # 主体结束位置

    # ---------- 底层接收与窗口管理 ----------

    async def _pull(self) -> bool:
        if self._eof:
            return False
        message = await self.receive()
        if message["type"] != "http.request":
            raise UploadError("无效的请求体消息")
        self._buf.extend(message.get("body", b""))
        if message.get("more_body") is False:
            self._eof = True
        return bool(message.get("body"))

    async def _index(self, needle: bytes) -> int:
        """在滚动缓冲中查找 needle，必要时继续拉取，找不到返回 -1。"""
        while True:
            pos = self._buf.find(needle)
            if pos >= 0:
                return pos
            if self._eof:
                return -1
            await self._pull()

    # ---------- part 头部 ----------

    @staticmethod
    def _parse_headers(blob: bytes) -> dict[str, str]:
        headers: dict[str, str] = {}
        for line in blob.split(b"\r\n"):
            if b":" not in line:
                continue
            key, value = line.split(b":", 1)
            headers[key.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()
        return headers

    @staticmethod
    def _parse_disposition(header: str | None) -> tuple[str, str | None]:
        name = ""
        filename: str | None = None
        if not header:
            raise UploadError("缺少 Content-Disposition 的表单字段")
        for piece in header.split(";"):
            piece = piece.strip()
            if piece.startswith("name="):
                name = piece[5:].strip('"')
            elif piece.startswith("filename="):
                filename = piece[9:].strip('"')
        if not name:
            raise UploadError("表单字段缺少 name")
        return name, filename

    async def _read_headers(self) -> bytes:
        while b"\r\n\r\n" not in self._buf:
            if len(self._buf) > MAX_HEADER_BYTES:
                raise UploadError("part 头部超过上限")
            if not await self._pull():
                raise UploadError("请求体在 part 头部处提前结束")
        raw, rest = self._buf.split(b"\r\n\r\n", 1)
        self._buf = bytearray(rest)
        return raw

    # ---------- part 主体 ----------

    async def iter_body(self, part: Part) -> AsyncIterator[bytes]:
        """吐出主体字节（不含结尾 CRLF），结束时消费整个 CRLF+边界标记。"""
        assert self._current is part
        while True:
            pos = await self._index(self._separator)
            if pos < 0:
                # 保留尾部前缀，防止 CRLF+边界被网络分片切开
                keep = min(len(self._separator) - 1, len(self._buf))
                if len(self._buf) - keep > PREFIX_MAX:
                    raise UploadError("未找到 multipart 边界，请求体格式错误")
                head = bytes(self._buf[:-keep]) if keep else bytes(self._buf)
                self._buf = self._buf[len(head):]
            else:
                head = bytes(self._buf[:pos])
                # 跳过 CRLF + 边界标记，留下 "--" 或 CRLF 供顶层判定
                del self._buf[: pos + len(self._separator)]
                self._current = None
            if head:
                self._emitted += len(head)
                if self._emitted > self.max_size:
                    raise UploadTooLarge(f"上传内容超过 {self.max_size} 字节上限")
                yield head
            if pos >= 0:
                return

    # ---------- 顶层迭代 ----------

    async def parts(self) -> AsyncIterator[Part]:
        # 文档起始边界（前面没有 CRLF）
        pos = await self._index(self._delimiter)
        if pos < 0:
            raise UploadError("请求体缺少 multipart 起始边界")
        del self._buf[: pos + len(self._delimiter)]

        while True:
            while len(self._buf) < 2 and not self._eof:
                await self._pull()
            if self._buf[:2] == b"--":
                return  # 结束标记 "--边界--"
            if self._buf[:2] == b"\r\n":
                del self._buf[:2]  # 还有下一个 part
            else:
                raise UploadError("multipart 边界后缺少 CRLF 或结束标记")

            raw_headers = await self._read_headers()
            headers = self._parse_headers(raw_headers)
            name, filename = self._parse_disposition(
                headers.get("content-disposition")
            )
            part = Part(
                name=name,
                filename=filename,
                content_type=headers.get("content-type"),
                headers=headers,
                _stream=self,
            )
            self._current = part
            yield part
            # 调用方未读完时兜底排空，保证边界被正确消费
            if self._current is not None:
                await part.drain()
