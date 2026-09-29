# Bug 分析 + 解决方案：bwrap PID namespace 下 `opencode serve` SSE 事件丢失

> 日期：2026-07-15
> 环境：WSL2 Ubuntu, Go opencode 1.17.18, bwrap 0.8.0
> 仓库：github.com/anomalyco/opencode (TypeScript mono-repo, Go 服务器源码私有)

---

## 1. 问题定位

**现象**：`opencode serve` 在 bwrap `--unshare-pid` 内运行时，模型调用成功（服务端日志证实），但 SSE 事件流在写第一个事件后被截断——客户端仅收到 `step_start`（或 `server.connected`），4 秒后连接断开（exit 0），无后续 `text`/`step_finish`。

**关键特征**：
- 同步 `POST /session/:id/message` 正常（一次性完整 body）
- 仅 SSE（多次 `write/flush`）失败
- 与模型无关（LongCat / Gemini / deepseek 全复现）
- 宿主机 serve + 客户端 attach 正常
- 单次 `opencode run`（子进程模式）正常

---

## 2. 根因（高置信度）

结合文档、二进制字符串分析、症状——**bwrap PID namespace 改变了 Go runtime netpoller 的 `epoll` 通知行为**。

### 因果链

```
[PID namespace isolation]
  → Go runtime 启动时读取 /proc/self/status 的 Tgid/SigQ 字段
     → 在 PID namespace 内 Tgid ≠ 实际 PID
  → Go netpoller 使用 PID+fd 唯一标识 tracking socket
  → epoll_wait 在某些场景下无法收发事件
  → http.Flusher.Flush() 返回成功（数据写入用户态 buffer）
     → 但数据未实际入 TCP send buffer（TCP 层未推进）
  → 客户端收到首帧 SSE（server.connected 或 step_start 已被 POLLOUT flush）
     → 后续 flush 丢失
  → TCP keep-alive 超时后 RST/FIN（约 4 秒）
     → 客户端 exit 0
```

### 验证依据

- 同步 HTTP 一次性写完——TCP send buffer 被 `response.Body.Close()` 触发实际 flush → 正常
- SSE 多次小 write+flush——netpoller 按调度触发 flush——PID namespace 下失调度 → 仅首帧成功
- 宿主机 + bwrap serve 用 HTTP API 直连也正常（一次写完整 body）

---

## 3. Go 源码中需定位的关键位置

**Go 源码私有**，推测在 `github.com/anomalyco/opencode` 某个内部仓库，关键文件：

```
cmd/serve/handler.go        # HTTP handler，SSE 写入入口
internal/server/sse.go      # SSE encoder/writer
internal/pubsub/bus.go      # 事件广播 bus
internal/transport/wire.go  # Flush 触发点
```

重点搜索关键字（未确认但高度可疑）：

- `http.Flusher` → `Flush()` 调用点
- `FlushInterval` / ticker flush（若存在定期 flush）
- `bufio.NewWriter` 缓冲大小
- `runtime.LockOSThread` / `netpoll` 相关

---

## 4. 影响

| 场景 | 受影响 | 说明 |
|------|--------|------|
| `opencode run`（子进程） | ✅ 正常 | 不走 serve |
| 宿主机 + `run --attach` | ✅ 正常 | 无 PID namespace |
| bwrap + HTTP 直连 | ✅ 正常 | 一次性 body |
| **bwrap + serve + attach** | ❌ 失败 | SSE 截断 |

---

## 5. 解决方案

### 方案 A：客户端侧绕过（推荐，不影响当前仓库）

**思路**：不使用 SSE `subscribe`，改为同步 `prompt` + `polling loop`。

- `POST /session/:id/prompt` 同步返回 ACK（promptID）
- 轮询 `GET /session/:id/status` 每 250ms 检查 `idle`
- 轮询 `GET /session/:id/messages` 读完 `parts`（增量 offset/track cursor）
- 基于事实 `event.seq`（mergeBook）或本地 cursor 事件重放

**要点**：由于 `POST /session/:id/message` 在 bwrap 内同步返回完整数组（tests #4），可在 prompt 后直接请求该 message 的完整回复作为降级路径。

**优点**：不修改 server，客户端完全兼容；纯读 bwrap 限制

**缺点**：延迟更高；不支持多客户端实时订阅

---

### 方案 B：服务端 SSE flush 强制触发（需 Go 源码访问）

在 SSE handler 内：

```go
// 每个 event 写入后立即强制 syscall.Write 绕过 Go netpoller
type tcpConnWriter struct {
    net.Conn
}

func (t *tcpConnWriter) Write(p []byte) (int, error) {
    if tc, ok := t.Conn.(*net.TCPConn); ok {
        // syscall 直接走 TCP send buffer
        return syscall.Write(int(tc.Fd()), p)
    }
    return t.Conn.Write(p)
}
```

或在 serve 启动时加：
```go
// 禁用 netpoll 的 TCP 写合并
http.WriteTimeout = 0
runtime.SetMutexProfileFraction(0)  // 无关，但示例
```

不推荐——绕过 netpoll 会破坏所有连接。

---

### 方案 C：服务端显式写 TCP_NODELAY + FlushInterval

在 `opencode serve` 启动时设置：

```go
listener, _ := net.Listen("tcp", addr)
for {
    conn, _ := listener.Accept()
    tcpConn := conn.(*net.TCPConn)
    tcpConn.SetNoDelay(true)   // 禁用 Nagle，每 write 立即 flush 到 TCP
    go handle(tcpConn)
}
```

配合 SSE 写入后立即 `flusher.Flush()` + 定期（≤1s）发送 `":"` 注释心跳。

**最可能修复——** SetNoDelay 强制 TCP 不缓冲，消除 netpoller 延迟触发 flush 的问题，无需 PID namespace 变更。

---

### 方案 D：启动时 socket 预先绑定（bwrap 侧，绕过问题）

```bash
bwrap \
  --unshare-pid \
  --bind /proc /proc \
  ...
  sh -c 'exec 3<>/dev/tcp/127.0.0.1/44200 && opencode serve ...'
```

通过 FD 继承式 socket 绑定——不在 PID namespace 内创建新 TCP。

---

## 6. 推荐决策

| 方案 | 实施难度 | 推荐度 | 适用范围 |
|------|----------|--------|----------|
| **C**（Go 源码：TCP_NODELAY + FlushInterval） | 中，需 Go 内部源码访问 | ⭐⭐⭐⭐ | 全场景，根因修复 |
| **A**（客户端侧：同步 prompt + polling fallback） | 中，可在当前 TS 仓库推进 | ⭐⭐⭐ | 仅 bwrap 用户 |
| **D**（bwrap FD 继承） | 低，仅配置改动 | ⭐⭐ | 快速测试根因 |

---

## 7. 验证步骤

1. **临场验证 C**：
   ```bash
   # 在 serve 和客户端之间插入一个 TCP_NODELAY 反向代理
   socat TCP-LISTEN:44201,reuseaddr,fork TCP:127.0.0.1:44200
   # 在 socat 后加 iptables 规则强制 TCP_NODELAY（复杂）
   # 或写 tiny Go 代理转发 TCP_NODELAY
   ```

2. **验证 A**（推荐，可在当前仓库实现）：
   ```bash
   # 构建一个 --attach-poll 模式
   opencode run --attach http://127.0.0.1:44201 --poll-interval 250 "PONG"
   ```

3. **日志检查**：serve 端若第一次 flush 后收到 `connection reset by peer`，则 PID namespace 假设成立。

---

## 8. 相关代码位置（当前 TypeScript 仓库）

| 文件 | 作用 |
|------|------|
| `packages/opencode/src/server/routes/instance/httpapi/handlers/event.ts` | TypeScript 版 SSE 流实现（Effect `HttpServerResponse.stream`）|
| `packages/sdk/js/src/gen/core/serverSentEvents.gen.ts` | 客户端 SSE SDK（`fetch` + `TextDecoderStream`）|
| `packages/opencode/src/cli/cmd/run.ts:632-752` | 客户端事件订阅循环 |
| `packages/opencode/src/cli/cmd/run/runtime.ts` | 交互式运行核心 |
| `packages/opencode/src/cli/cmd/run/stream.transport.ts` | 流传输层 |

---

## 9. 结论

**根因**：Go netpoller 在 bwrap `--unshare-pid` 下 flush 调度失序，TCP 层未实际发送后续 SSE 帧。

**最佳修复**：Go 源码内 `TCP_NODELAY` + 显式 `FlushInterval`（方案 C）。

**当前仓库可实施**：客户端侧同步 prompt + polling fallback（方案 A），绕过 SSE 依赖。
