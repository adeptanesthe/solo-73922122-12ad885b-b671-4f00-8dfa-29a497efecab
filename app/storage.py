"""本地文件存储。

- 上传先写 tmp/ 下的随机临时文件，全部成功并落库后才原子 rename 到 files/；
  上传失败只删除自己的临时文件，绝不发布链接。
- 清理只接受数据库查出的 storage_name，并做严格名校验，
  保证不可能误删其他交接文件或目录之外的任何路径。
- 路径统一从 config 模块动态读取，便于测试切换独立数据目录。
"""

from __future__ import annotations

import os
import re
import secrets
import time
from pathlib import Path

from . import config
from .config import ensure_dirs

_STORAGE_RE = re.compile(r"^[0-9a-f]{32}$")


def new_storage_name() -> str:
    return secrets.token_hex(16)


def new_tmp_path() -> Path:
    ensure_dirs()
    return config.TMP_DIR / f"{secrets.token_hex(16)}.part"


def finalize(tmp_path: Path, storage_name: str) -> Path:
    """临时文件原子发布到正式目录。"""
    if not _STORAGE_RE.match(storage_name):
        raise ValueError("非法的存储文件名")
    ensure_dirs()
    target = config.FILES_DIR / storage_name
    os.replace(tmp_path, target)
    return target


def storage_path(storage_name: str) -> Path | None:
    """把数据库中的存储名解析为安全路径；任何异常名字一律拒绝。"""
    if not _STORAGE_RE.match(storage_name):
        return None
    path = (config.FILES_DIR / storage_name).resolve()
    if path.parent != config.FILES_DIR.resolve():
        return None
    return path


def safe_unlink_tmp(path: Path) -> None:
    try:
        resolved = path.resolve()
        if resolved.parent == config.TMP_DIR.resolve() and resolved.is_file():
            resolved.unlink()
    except FileNotFoundError:
        pass


def safe_remove_storage(storage_name: str) -> bool:
    """删除指定交接文件；不属于 files/ 目录或不存在时不做任何操作。"""
    path = storage_path(storage_name)
    if path is None:
        return False
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def cleanup_stale_tmp(max_age_seconds: int = 3600) -> None:
    """进程启动时清掉遗留的临时文件（上次进程在上传中途被杀死）。"""
    ensure_dirs()
    now = time.time()
    for entry in config.TMP_DIR.iterdir():
        try:
            if entry.is_file() and now - entry.stat().st_mtime > max_age_seconds:
                entry.unlink()
        except OSError:
            pass
