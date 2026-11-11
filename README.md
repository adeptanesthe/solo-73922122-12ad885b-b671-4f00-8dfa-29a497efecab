# 敏感证据限时交接

应急响应人员向协作方交接敏感证据时使用的一次性、限时、单浏览器绑定交接服务。

- 上传单文件（上限 **16 MiB**），生成 **10 分钟有效**的随机链接；
- 打开链接只查询状态，**不消耗授权**；点击“领取”才把证据绑定到当前浏览器的
  随机 **HttpOnly** 会话；
- 同一链接并发领取**只有一个浏览器成功**；原浏览器重试仍取得同一份授权；
- 领取后仅该会话可下载，**续传窗口另计 10 分钟**（刷新接收页保留授权并顺延），
  下载支持**单段 HTTP Range** 断点续传；
- 授权落 **SQLite**、文件存本地；链接令牌与会话秘密**只存 SHA-256 哈希**；
- 运行**两个 Web 进程**共享同一数据卷，领取与到期判断基于同一数据库，结论一致，
  重启后状态保留；
- 未领取链接或续传会话到期后拒绝新下载并**自动清理文件**；到期前已获准的下载凭
  租约保护可完整结束，清理**不会误删**其他交接文件；
- 上传、下载**全程分块流式处理**（256 KiB），不做全量内存缓冲；上传失败不发布链接。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8080
```

编排包含 3 个容器：

| 容器 | 角色 |
|---|---|
| `handoff-web1` / `handoff-web2` | 两个独立的 FastAPI/uvicorn Web 进程，共同挂载 `handoff-data` 卷 |
| `handoff-gateway` | Nginx 前置入口（`8080`），轮询两个 Web 进程，关闭请求/响应缓冲 |

SQLite 数据库与证据文件都在命名卷 `handoff-data` 中，容器重启或重建后状态保留。

本地开发（无需 Docker）：

```bash
pip install -r requirements.txt
HANDOFF_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
# 再用另一个端口起第二个进程，HANDOFF_DATA_DIR 指向同一目录即可
```

## 浏览器演示完整交接

1. 打开 `http://localhost:8080`，进入**发送证据**页，选择 ≤16 MiB 文件上传；
2. 上传成功后得到形如 `http://localhost:8080/receive/<随机令牌>` 的链接，复制给协作方；
3. 协作方在浏览器 A 打开链接——页面只显示文件名与大小，授权尚未被消耗；
4. 点击**领取并绑定本浏览器**：服务端写入随机 HttpOnly 会话，页面出现下载按钮；
5. 在浏览器 B 打开同一链接：提示“已被其他浏览器领取”，无法领取或下载；
6. 浏览器 A 刷新页面授权仍在；点击下载，中断后重新下载即凭 `Range` 从断点续传；
7. 10 分钟未领取、或领取后续传窗口到期，证据文件被后台任务自动删除。

## 测试

```bash
pip install httpx
python3 tests/test_handoff.py
```

测试包含 14 个用例：自研 multipart 流式解析器（1 字节/块切碎、边界跨块、超限与
畸形报文）、完整交接流程、8 客户端并发领取恰好一个成功、跨“进程”状态一致、
单段 Range/206/416、两类到期清理、下载租约保护、清理隔离、失败上传不发布链接、
库内仅存哈希。

## 安全模型与一致性设计

**秘密只存哈希。** 链接令牌和下载会话都是 32 字节 CSPRNG 随机数（base64url），
仅随链接 / HttpOnly Cookie 下发；数据库的 `link_hash`、`session_hash` 列保存
SHA-256，库文件泄露也无法还原可用秘密。

**领取的原子性。** 两个 Web 进程共享同一 SQLite（WAL 模式）。领取在
`BEGIN IMMEDIATE` 写事务内执行
`UPDATE ... WHERE id=? AND session_hash IS NULL`，写锁让并发领取串行化：
抢到的进程绑定成功，其余进程读到“已领取”而得到 409。所有到期判断统一使用数据库
时钟 `unixepoch()`，因此两个进程的结论始终一致。

**流式处理。** 上传使用自研有界内存 multipart 解析器（`app/multipart_stream.py`），
每次只拉取一个 ASGI 网络分片、滚动匹配边界，文件块直接写入 `tmp/` 临时文件；
全部成功并落库后才 `os.replace` 原子发布到 `files/`，任何失败都只清理自己的临时
文件、绝不产生链接。下载用 `StreamingResponse` 按 256 KiB 分块读盘，支持
`bytes=start-end` 单段 Range。

**到期与清理。** 后台线程每 15 秒在同一把写锁内：先删除过期下载租约，再挑出
“未领取且链接到期”或“已领取且续传会话到期”的记录；仅当该记录没有未结束的下载
租约时才删库行并按清单删除对应文件。下载请求获准后先登记租约，保证到期前已放行
的下载能完整传完；物理删除只接受数据库查出的 32 位十六进制存储名并做路径校验，
不可能触碰其他交接文件或目录外路径。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/`、`/send`、`/receive/{token}` | 首页 / 发送页 / 接收页 |
| POST | `/api/handoffs` | `multipart/form-data` 流式上传单文件，成功返回随机链接 |
| GET | `/api/handoffs/{token}/status` | 查询状态，不绑定、不消耗授权 |
| POST | `/api/handoffs/{token}/claim` | 领取：原子绑定随机 HttpOnly 会话（幂等限于原会话） |
| GET | `/api/handoffs/{token}/download` | 凭会话下载，支持单段 `Range` |
| GET | `/healthz` | 健康检查 |

时效与数据目录可用环境变量调整：`HANDOFF_LINK_TTL`、`HANDOFF_GRANT_TTL`、
`HANDOFF_LEASE_TTL`（秒）、`HANDOFF_DATA_DIR`。

## 目录结构

```
app/
  config.py             # 上限、时效、路径配置
  security.py           # 随机令牌、SHA-256 哈希、文件名净化
  multipart_stream.py   # 有界内存 multipart 流式解析器
  storage.py            # 临时文件/正式文件原子发布与安全清理
  database.py           # SQLite 模式、原子领取、到期清理与下载租约
  main.py               # FastAPI 路由与后台清理线程
  static/               # 首页、发送页、接收页、样式
tests/test_handoff.py   # 14 项端到端与解析器测试
nginx.conf              # 前置入口：轮询两进程、关闭缓冲
Dockerfile
docker-compose.yml      # web1 + web2 + gateway，共享 handoff-data 卷
```
