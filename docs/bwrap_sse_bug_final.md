# OpenCode Bug 分析：bwrap PID namespace 下 SSE 事件丢失

> **日期**：2026-07-15 | **状态**：分析完成，待验证修复
> **环境**：WSL2 Ubuntu, opencode 1.17.18 (TypeScript + Effect + Bun), bwrap 0.8.0

---

## 1. 现象概述

`opencode serve` 在 bwrap `--unshare-pid` 内运行时，模型调用本身成功完成（服务端日志证实），但 **SSE 事件流在首帧后被截断**——客户端仅收到 `step_start`，4 秒后连接断开（exit 0），无后续 `text`/`step_finish` 事件。

### 关键行为矩阵

| # | 场景 | 结果 | 说明 |
|---|------|------|------|
| 1 | `opencode run` 直接在 bwrap 内 | ✅ 正常 | 不走 serve/SSE |
| 2 | 宿主机 serve + `run --attach` | ✅ 正常 | 无 PID namespace |
| 3 | bwrap serve + HTTP `POST /session/:id/message` | ✅ 正常 | 同步一次性 body |
| 4 | **bwrap serve + `run --attach`** | ❌ **仅 step_start** | SSE 流截断 |
| 5 | 多模型（LongCat / Gemini / deepseek）均复现 | ❌ | 与模型无关 |

---

## 2. 技术栈确认

> **重要修正**：原始 bug report 错误地描述 opencode 为 "Go ELF binary"。实际上：

- **语言**：TypeScript（Effect 框架）
- **运行时**：Bun 1.3.14 compile 产物（`package.json:7` → `"packageManager": "bun@1.3.14"`）
- **HTTP server**：Node.js `createServer()`（`server.ts:8` → `import { createServer } from "node:http"`）
- **流处理**：Effect `Stream` + `Queue` + fiber 调度

任何基于 "Go netpoller / goroutine" 的分析方向均不适用。

---

## 3. SSE 数据流梳理

### 3.1 服务端 SSE 写入路径

```
EventV2Bridge.listen()
  → events.listen(callback)              # event-v2-bridge.ts:37
    → GlobalBus.emit("event", payload)     # event-v2-bridge.ts:41

SSE endpoint (event.ts):
  Queue.unbounded<EventV2.Payload>()       # event.ts:34
  events.listen(event =>
    Effect.sync(() =>
      Queue.offerUnsafe(queue, event)      # event.ts:35  ← unsafe 写入
    )
  )
  Stream.fromQueue(queue)                  # event.ts:37  ← fiber 等待
    .pipe(Stream.filter(...))
    .pipe(Stream.map(...))

  output = stream.pipe(
    Stream.merge(disposed),
    Stream.takeUntil(...)
  )

  heartbeat = Stream.tick("10 seconds")    # event.ts:66

  HttpServerResponse.stream(
    Stream.make({type: "server.connected"})  # event.ts:73 ← 首帧（同步）
      .pipe(Stream.concat(                   # event.ts:74 ← 后续（异步 queue）
        output.pipe(Stream.merge(heartbeat))
      ))
      .pipe(Stream.map(eventData))
      .pipe(Sse.encode())
      .pipe(Stream.encodeText)
  )
```

### 3.2 同步 prompt 端点对比

```
session.ts:304:
  HttpServerResponse.stream(
    Stream.make(JSON.stringify(message))    # 单元素 stream，无 queue 等待
      .pipe(Stream.encodeText)
  )
```

### 3.3 客户端消费路径

```
run.ts:764:  const events = await client.event.subscribe()
run.ts:765:  loop(client, events)  # for await (const event of events.stream)
run.ts:787:  const result = await client.session.prompt(...)  # 并行：prompt + SSE
```

---

## 4. 根因分析

### 4.1 直接原因（高置信度）

**`Queue.offerUnsafe` + `Stream.fromQueue` 的 fiber 唤醒在 bwrap PID namespace 下失效**。

因果链：

1. SSE 连接建立 → `Stream.make(server.connected)` 同步 emit → **首帧成功到达客户端** ✅
2. `Stream.concat` 切换到 `Stream.fromQueue(queue)` → 队列空 → **fiber 挂起等待** `Queue.take`
3. 模型调用在后台完成 → `EventV2Bridge.listen` 回调触发
4. `Queue.offerUnsafe(queue, event)` 被调用：
   - 数据成功写入 `MutableQueue` 内部
   - 需要唤醒挂起的 taker fiber（resolve Deferred）
5. **在 bwrap PID namespace 下，唤醒信号未能传递到挂起的 fiber** → 数据永远不被消费 → SSE 流停滞
6. 4 秒后连接超时断开

### 4.2 为什么首帧能成功

`Stream.make({type: "server.connected"})` 是**同步构造**的单元素 stream，不经过 queue，不需要 fiber 唤醒。`Stream.concat` 消耗完第一个 stream 后才切换到第二个（queue-backed stream），此时才触发阻塞。

### 4.3 为什么同步 API 正常

`POST /session/:id/message`（`session.ts:304`）使用 `Stream.make(JSON.stringify(message))` — 也是单元素同步 stream，**不经过 queue**，不涉及 fiber 挂起/唤醒。

### 4.4 PID namespace 影响机制（推测，需验证）

可能的影响路径：

1. **PID 1 行为差异**：bwrap `--unshare-pid` 内进程为 PID 1。PID 1 不会因无人收割子进程而收到 SIGCHLD 的默认行为不同，且不会被 SIGTERM/SIGINT 以外的信号杀死。如果 Bun/JSC 的事件循环依赖某些信号行为来 drain microtask queue，则可能受影响。

2. **timerfd / signalfd 在 PID namespace 边界的行为差异**：Effect 的 fiber 调度器底层依赖 Bun 的事件循环（基于 JSC + libuv）。如果 `epoll_wait` 监听的 timerfd/signalfd 在 PID namespace 下行为异常，可能导致 Promise resolve 后的 microtask 不被及时 drain。

3. **`/proc` 信息不完整**：bwrap 挂载了 `--proc /proc`，但 PID namespace 下 `/proc/self/` 信息与宿主机不同，可能影响运行时的进程状态判断。

### 4.5 排除的因素

| 因素 | 排除理由 |
|------|----------|
| 模型 provider | 3 个不同模型全部复现 |
| 网络连通性 | bwrap 内 curl 可访问外部 API |
| Go runtime | opencode 不是 Go 程序 |
| Socket 创建 | 首帧写入成功，说明 TCP 连接正常 |
| HTTP server 层 | 同步 API 在同一 server 上正常工作 |

---

## 5. 次要问题：keepAliveTimeout vs heartbeat 不匹配

### 位置

`server.ts:192`：`const server = createServer()` — 无参数，Node.js 默认 `keepAliveTimeout = 5000ms`

`event.ts:66`：`Stream.tick("10 seconds")` — heartbeat 间隔 10 秒

### 分析

- Node.js `http.createServer()` 默认 `keepAliveTimeout = 5000`（5 秒）
- SSE heartbeat 10 秒才发一次，**如果** `keepAliveTimeout` 作用于 SSE 长连接，则 heartbeat 永远无法在 5 秒前发出
- **但**：SSE 连接在首帧写入后进入 chunked transfer encoding，通常不受 `keepAliveTimeout` 影响（该超时主要作用于 keep-alive 空闲连接）
- **影响评估**：中低——作为防御性修复仍值得做，但可能不是导致此 bug 的直接原因

---

## 6. 解决方案

### 方案 A（推荐）：替换 `Queue.offerUnsafe` 为 Effect 正规原语

**原理**：`Queue.offerUnsafe` 绕过了 Effect runtime 的 fiber 调度路径。改用 `PubSub.publish`（正规 Effect primitive）或 `Stream.callback`（已在同文件 `disposed` 变量中使用），让唤醒信号走标准运行时路径。

**修改文件**：`packages/opencode/src/server/routes/instance/httpapi/handlers/event.ts`

**当前代码**（event.ts:34-37）：
```typescript
const queue = yield* Queue.unbounded<EventV2.Payload>()
const unsubscribe = yield* events.listen((event) => Effect.sync(() => Queue.offerUnsafe(queue, event)))
yield* Effect.addFinalizer(() => unsubscribe)
const stream = Stream.fromQueue(queue).pipe(...)
```

**方案 A-1：使用 `Stream.callback`**（与同文件 `disposed` 变量风格一致）：

```typescript
const stream = Stream.callback<EventV2.Payload>((queue) => {
  return Effect.gen(function* () {
    const unsubscribe = yield* events.listen((event) =>
      Effect.sync(() => Queue.offerUnsafe(queue, event))
    )
    return Effect.sync(() => unsubscribe)
  }).pipe(Effect.map((cleanup) =>
    Effect.acquireRelease(Effect.void, () => cleanup)
  )).pipe(Effect.flatten)
}).pipe(...)
```

注：`Stream.callback` 内部也使用 `Queue.offerUnsafe`，但其 queue 生命周期由 `Stream.callback` 自身管理，可能在实现细节上与手动 `fromQueue` 有差异。

**方案 A-2：使用 PubSub（更彻底）**：

```typescript
const pubsub = yield* PubSub.unbounded<EventV2.Payload>()
const unsubscribe = yield* events.listen((event) =>
  PubSub.publish(pubsub, event)  // 正规 Effect，走标准 fiber 调度
)
yield* Effect.addFinalizer(() => unsubscribe)
const stream = Stream.fromPubSub(pubsub).pipe(...)
```

**优点**：`PubSub.publish` 是正规 Effect primitive，走标准 fiber 调度路径，不依赖 `offerUnsafe` 的 unsafe 语义。

**风险**：低——`PubSub` 是 Effect 的核心原语，与 `Queue` 语义兼容。

### 方案 B：keepAliveTimeout 防御性修复

**修改文件**：`packages/opencode/src/server/routes/instance/httpapi/handlers/event.ts` 或 `server.ts`

**选项 1**：在 `server.ts:192` 后设置：
```typescript
const server = createServer()
server.keepAliveTimeout = 0  // SSE 长连接不需要 keepAliveTimeout
```

**选项 2**：缩短 heartbeat 到 3 秒（小于默认 5 秒）：
```typescript
// event.ts:66
const heartbeat = Stream.tick("3 seconds").pipe(...)
```

### 方案 C：客户端侧 polling fallback（绕过方案）

如果服务端修复不可行，客户端可以绕过 SSE：

1. `POST /session/:id/prompt` 发起请求（同步等待完成）
2. 直接使用返回的 JSON body（包含完整 `parts` 数组）
3. 不依赖 SSE 事件流

**缺点**：丧失实时流式输出能力。

---

## 7. 验证计划

### 7.1 确认根因

```bash
# 在 bwrap 内启动 serve，同时监听 strace
strace -f -e trace=epoll_wait,timerfd_create,timerfd_settime,signalfd \
  bwrap --unshare-pid --dev /dev \
  --ro-bind /usr /usr --ro-bind /lib /lib --ro-bind /lib64 /lib64 \
  --ro-bind /bin /bin --ro-bind /etc /etc \
  --bind ~/.opencode ~/.opencode \
  --bind ~/.local/share/opencode ~/.local/share/opencode \
  --bind ~/.config/opencode ~/.config/opencode \
  --bind /tmp /tmp --proc /proc \
  env HOME=$HOME OPENCODE_SERVER_PASSWORD=test \
    opencode serve --port 44200 --hostname 127.0.0.1 2>strace.log
```

对比宿主机和 bwrap 内的 `epoll_wait` 行为。

### 7.2 验证修复

1. 应用方案 A-2（PubSub 替换）
2. 重新构建：`bun run build`
3. 在 bwrap 内启动 serve
4. 使用 `run --attach` 测试 SSE 流

```bash
OPENCODE_SERVER_PASSWORD=test timeout 60 opencode run --attach http://127.0.0.1:44200 \
  -m new_api/deepseek-v4-pro --format json --auto \
  "Reply EXACTLY: PONG"
```

**预期**：收到完整 NDJSON 流（`step_start` → `text` → `step_finish`）。

### 7.3 回归验证

- 宿主机 serve + `run --attach`（不应退化）
- 宿主机 serve + HTTP 同步 API（不应退化）
- `opencode run` 直接运行（不应退化）

---

## 8. 相关源码位置

| 文件 | 作用 | 关键行 |
|------|------|--------|
| `packages/opencode/src/server/routes/instance/httpapi/handlers/event.ts` | SSE 事件流端点 | L34-35（Queue + offerUnsafe），L66（heartbeat），L72-77（Stream.concat） |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/global.ts` | 全局 SSE 端点 | L38-39（同样使用 Stream.callback + offerUnsafe） |
| `packages/opencode/src/server/server.ts` | HTTP server 创建 | L192（createServer 无参数） |
| `packages/opencode/src/event-v2-bridge.ts` | 事件桥接层 | L37-67（listen → GlobalBus.emit） |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts` | 同步 prompt 端点 | L304（Stream.make 单元素，无 queue） |
| `packages/opencode/src/cli/cmd/run.ts` | 客户端 `run --attach` | L764（event.subscribe），L632-753（SSE 消费循环） |

---

## 9. 总结

| 维度 | 结论 |
|------|------|
| **根因** | Effect `Queue.offerUnsafe` + `Stream.fromQueue` 的 fiber 唤醒在 bwrap PID namespace 下失效 |
| **影响范围** | 仅 SSE 流端点（`/event`）在 bwrap 内受影响 |
| **推荐修复** | 方案 A-2：`Queue` → `PubSub` 替换，让 publish 走正规 Effect fiber 调度 |
| **紧急绕过** | 使用 `POST /session/:id/message` 同步 API 替代 SSE |
| **置信度** | 根因方向：高（源码证据充分）；PID namespace 影响机制：中（需 strace 验证） |
