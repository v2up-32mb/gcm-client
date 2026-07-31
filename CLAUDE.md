# GCM Client (Android)

Android VPN 客户端，基于 GCM (Go Cloud Multiplexer) 2 字节头二进制流复用协议。
从 [ech-client](https://github.com/v2up-32mb/ech-client) 改造而来，复用其 Profile 管理与 VPN 隧道框架，替换核心 Go 库与协议层。

## 技术栈

- **Android 层**：Java 17 / AGP 8.x / Material / ZXing，包名 `com.gcm.client.app`，`applicationId` 同名
- **Go 核心库**：`golib/`（`module gcm`，Go 1.24），通过 `gomobile bind` 编译为 `app/libs/gcm.aar`
- **VPN 隧道**：[hev-socks5-tunnel](https://github.com/heiher/hev-socks5-tunnel)（CI 阶段 `git clone` 到 `app/src/main/jni`）
- **SOCKS5 / HTTP 代理**：由 Go 核心库在本地监听，`hev-socks5-tunnel` 将 VPN Tun 流量转发到该 SOCKS5

## 核心协议

GCM 二进制多路复用协议（2 字节头）：

```
[STREAM_ID:1][TYPE:1][可选 DATA]
TYPE = 0 CONNECT    DATA = ASCII "host:port|"
TYPE = 1 CONNECTED  无 DATA
TYPE = 2 DATA       DATA = 任意二进制
TYPE = 3 CLOSE       无 DATA
```

WebSocket 连接：`wss://<workerHost>/<userID>?fallbackip=<出口IP列表>`

- 多条 WS 连接共享同一个 `GCMPool`
- `STREAM_ID` 为 1 字节，最多 256 个流，由本地分配
- 乐观响应：发送 CONNECT 后立即回 SOCKS5 成功，不等 CONNECTED 返回

## 字段映射（用户配置 → GCM 语义）

| Android pref key | 含义 | Go 参数对应 | Worker 协议 |
|---|---|---|---|
| `WorkerHost` | Worker 域名（如 `gcm.ics.de5.net`） | `wsServer` | TLS SNI + Host 头 |
| `UserId` | 用户路径标识（如 `v2up`） | `uid` | WS URL 路径 |
| `PrefIp`（优选中转节点） | 逗号分隔多个 `IP:端口`，TCP 中继点 | `relay` | TLS SNI 保持 WorkerHost |
| `FallbackIp`（出口代理 IP） | 逗号分隔多个，透传给 Worker | `fip` | `?fallbackip=` 查询参数 |
| `WsConn` | 每个中转 IP 的 WebSocket 连接数 | `n` | — |

### 与 GCM CLI 的对应

- `PREF_IP` ↔ GCM CLI `--relay/-r`（`RelayIPs`）：TCP 中转入口
- `FALLBACK_IP` ↔ GCM CLI `--proxy-ip/-p`（`ProxyIP`）：出口端代理 IP
- `WORKER_HOST` ↔ GCM CLI `--worker/-w`（`WorkerHost`）
- `USER_ID` ↔ GCM CLI WS URL 路径

## 配置导出/导入 URI

格式（`gcm://`，兼容 `ech://` 导入）：

```
gcm://<workerHost>?ip=<优选中转IP:端口>&fip=<出口代理IP>&user_id=<用户ID>#<配置名称>
```

- `ip` 对应 `PREF_IP`（可选，逗号分隔多个）
- `fip` 对应 `FALLBACK_IP`（可选，逗号分隔多个）
- `user_id` 对应用户路径标识（`token` 作为旧别名也兼容）
- 支持 URL fragment 编码配置名称

## 构建

### 本地构建

需要 JDK 17、Android SDK / NDK、Go 1.24+：

```bash
cd golib
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init
gomobile bind -target=android -androidapi=24 -o ../app/libs/gcm.aar
cd ..
./gradlew assembleDebug
```

### CI 构建（GitHub Actions）

- `.github/workflows/build-debug.yml`：push 到 `feat/*` / `fix/*` 或 `workflow_dispatch` 时触发
  - 自动 `git clone` `hev-socks5-tunnel` 到 `app/src/main/jni`
  - 条件编译 `gcm.aar`（`gomobile bind`）并跳过 if 已存在
  - 编译并按 ABI 上传 APK artifacts
- `.github/workflows/release.yml`：push `v*` tag 时触发，对 release APK 签名并发布 GitHub Release
  - 需配置仓库 Secrets：`SIGNING_KEY` / `ALIAS` / `KEY_STORE_PASSWORD` / `KEY_PASSWORD`

## 目录结构

```
golib/
  gcm.go        核心 Go 库（gomobile 入口 StartSocksProxy/StopSocksProxy）
  go.mod        module gcm, 依赖 gorilla/websocket
app/
  libs/gcm.aar  CI 编译产物（不入库）
  src/main/jni/ hev-socks5-tunnel（CI 克隆入库）
  src/main/java/com/gcm/client/app/  Android UI/Service
  src/main/res/                      布局/字符串/图标
.github/workflows/  CI 工作流
```

## Go 核心 API

`StartSocksProxy(listenAddr, wsServer, n, relay, uid, fip, verbose)` 启动本地 SOCKS5 代理：
- `listenAddr`：本地监听地址（`127.0.0.1:port`）
- `wsServer`：Worker 域名（无 `wss://` 前缀）
- `n`：每个中转 IP 的 WebSocket 连接数（默认 1）
- `relay`：优选中转节点地址（`host:port` 或 `IP:port`，逗号分隔多个；空则直连 Worker）
- `uid`：WS URL 路径标识
- `fip`：出口代理 IP（逗号分隔多个，通过 `?fallbackip=` 透传给 Worker）
- `verbose`：日志开关

`StopSocksProxy()` 停止代理并关闭连接池。
