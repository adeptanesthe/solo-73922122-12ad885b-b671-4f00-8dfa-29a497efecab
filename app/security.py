"""随机秘密与哈希工具。

链接令牌与会话秘密都使用 32 字节的 CSPRNG 随机数，URL 中只出现原始随机值；
数据库内只保存其 SHA-256 哈希，库文件泄露也无法还原出可使用的链接或会话。
"""

from __future__ import annotations

import base64
import hashlib
import secrets

_RANDOM_BYTES = 32


def new_token() -> str:
    """生成 URL 安全的随机令牌（原始值，仅随链接 / Cookie 下发一次）。"""
    return base64.urlsafe_b64encode(secrets.token_bytes(_RANDOM_BYTES)).rstrip(b"=").decode("ascii")


def hash_token(token: str) -> str:
    """计算令牌的 SHA-256 十六进制哈希（入库形态）。"""
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def safe_filename(raw_name: str | None) -> str:
    """剥离目录部分与危险字符，避免任何路径穿越；为空时给出通用名。"""
    if not raw_name:
        return "evidence.bin"
    name = raw_name.replace("\\", "/").split("/")[-1].strip()
    # 去掉控制字符与 shell/路径敏感字符
    cleaned = "".join(c for c in name if c.isprintable() and c not in '"\'`$;|<>*?{}').strip()
    cleaned = cleaned.lstrip(".")
    return cleaned or "evidence.bin"
