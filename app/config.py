"""运行配置：所有路径与时效均可用环境变量覆盖。

两个 Web 进程挂载同一份数据卷（SQLite + 文件目录），因此配置默认值一致。
"""

from __future__ import annotations

import os
from pathlib import Path

# 单次上传硬上限：16 MiB
MAX_UPLOAD_BYTES = 16 * 1024 * 1024

# 链接未领取时的有效期（秒）：10 分钟
LINK_TTL_SECONDS = int(os.environ.get("HANDOFF_LINK_TTL", str(10 * 60)))

# 领取后续传有效期（秒）：10 分钟，每次刷新接收页 / 状态查询都会顺延
GRANT_TTL_SECONDS = int(os.environ.get("HANDOFF_GRANT_TTL", str(10 * 60)))

# 已获准下载的租约时长：保证一个已经放行的下载在中途不会被清理打断
LEASE_TTL_SECONDS = int(os.environ.get("HANDOFF_LEASE_TTL", str(30 * 60)))

# 流式读写分片大小：256 KiB，绝不整体缓冲文件
CHUNK_SIZE = 256 * 1024

DATA_DIR = Path(os.environ.get("HANDOFF_DATA_DIR", "/data"))
DB_PATH = DATA_DIR / "handoff.sqlite3"
FILES_DIR = DATA_DIR / "files"
TMP_DIR = DATA_DIR / "tmp"

COOKIE_NAME = "handoff_session"


def ensure_dirs() -> None:
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
