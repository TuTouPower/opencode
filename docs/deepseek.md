# OpenCode bwrap SSE Bug：根因分析与解决方案

> **日期**：2026-07-15 | **源码分析**：opencode TypeScript 源码

---

## 0. 根本前提修正

用户文档假设 opencode 是 Go 编译的二进制——**这个前提是错的**。opencode 1.17.18 是 **TypeScript + Effect 框架 + Bun compile** 产物（见 `package.json`: `"packageManager": "bun@1.3.14"`）。文档中"Go netpoller / goroutine"方向的根因分析全盘错误，下面重新分析。

---

## 1. Bug #1（高置信度，直接原因）：`Stream.concat` + `Stream.fromQueue` 的 fiber 唤醒在 bwrap PID namespace 下失效

### 位置

`packages/opencode/src/server/routes/instance/httpapi/handlers/event.ts:72-77`

```typescript
return HttpServerResponse.stream(
  Stream.make({ id: eventID(), type: "server.connected", properties: {} }).pipe(
    Stream.concat(output.pipe(Stream.merge(heartbeat, { haltStrategy: "left" }))),
    Stream.map(eventData),
    Stream.pipeThroughChannel(Sse.encode()),
    Stream.encodeText,
    Stream.ensuring(Effect.sync(() => log.info("event disconnected"))),
  ),
  ...
)
```

### 机制

1. `Stream.concat` 先发出初始 `server.connected`，然后切换到 `output`（来自 `Stream.fromQueue` 的事件流，`event.ts:34`）
2. HTTP response writer 拉第一个 chunk（`server.connected`）→ 写入成功 → 客户端收到 `step_start`
3. 拉第二个 chunk 时，`fromQueue` 队列为空，**fiber 挂起等待**
4. 模型调用完成后事件 publish → `EventV2Bridge.listen()` 回调 → `Queue.offerUnsafe(queue, event)` 被调用（`event.ts:35`）
5. `offerUnsafe` 操作内部 `MutableQueue` 并 resolve Deferred。但 **taker fiber 的唤醒信号在 bwrap PID namespace 下无法正确传递**，挂起的 fiber 永远收不到

### 为什么在 PID namespace 下有差异

Effect 框架中 `Queue.offerUnsafe` 直接操作内部状态并 resolve Deferred。Deferred 的 resolve 依赖 Promise/microtask 机制。Bun 运行时（JSC + libuv）在 bwrap PID namespace 中作为 PID 1 运行时，microtask 调度行为与正常进程不同。具体而言：

- PID 1 进程不会被内核发送 `SIGKILL` 以外的信号，孤儿进程的默认信号行为不同
- Bun/JSC 内部的 `epoll_wait` / timerfd / signalfd 等机制在 PID namespace 边界上有已知差异
- 如果 microtask 队列的 drain 依赖某个内部 IO 回调（如 timerfd），而该回调在 PID namespace 下丢失，则 `offerUnsafe` 写入的数据永远不会被 taker fiber 消费

### 证据吻合

| 测试场景 | 结果 | 吻合度 |
|----------|------|--------|
| 首个 SSE chunk（server.connected / step_start）到达客户端 | ✓ | `Stream.concat` 的第一个元素不依赖 queue，直接 emit |
| 模型调用完成，服务端日志显示 "exiting loop" | ✓ | Events 已 publish 到 queue，但无法送达 HTTP stream writer |
| `POST /session/:id/message` 同步 API 正常 | ✓ | `session.ts:304` 用 `Stream.make(JSON.stringify(message))`，单元素 stream，无 await-queue 阻塞 |
| `opencode run` 直接跑 bwrap 内正常 | ✓ | 不走 serve 模式，不走 SSE |
| 多模型均复现 | ✓ | 与模型无关，是事件传输层问题 |

---

## 2. Bug #2（中置信度，加剧因素）：`keepAliveTimeout` 5s 与 heartbeat 10s 不匹配

### 位置

`packages/opencode/src/server/server.ts:192`

```typescript
const server = createServer()  // 无参数，Node.js 默认 keepAliveTimeout = 5000ms
```

### 机制

- Node.js `http.createServer()` 默认 `keepAliveTimeout = 5000`（5 秒）
- SSE heartbeat 在 `event.ts:66` 设置为 **10 秒**：`Stream.tick("10 seconds")`
- 闲置 5 秒后 `keepAliveTimeout` 触发，关闭连接
- heartbeat 10 秒才发一次，**永远无法在 5 秒前发出**，heartbeat 形同虚设

**影响**：即使 Bug #1 修复后，SSE 连接在空闲时仍可能在 5 秒后被断开。

---

## 3. Bug #3（备选方向，低置信度）：`offerUnsafe` 的 unsafe 语义在 PID namespace 下副作用不确定

### 位置

`packages/opencode/src/server/routes/instance/httpapi/handlers/event.ts:34-35`

```typescript
const queue = yield* Queue.unbounded<EventV2.Payload>()
const unsubscribe = yield* events.listen((event) => Effect.sync(() => Queue.offerUnsafe(queue, event)))
```

### 机制

- `Queue.offerUnsafe` 是 unsafe 版本，不经过 Effect runtime fiber 调度
- 直接修改内部 `MutableQueue` 状态
- 在 bwrap PID namespace 下，如果 fiber 调度器的内部状态机依赖某些系统调用（timerfd / signalfd / epoll），PID namespace 导致行为差异
- `Stream.fromQueue` 内部通过 `Queue.take` 拉取数据。`take` 在队列空时挂起 fiber。`offerUnsafe` 的唤醒信号可能无法正确传递到挂起的 fiber

**修复方向**：用 `PubSub` 替代 `Queue`，`PubSub.publish` 是正经 Effect，走正规运行时调度路径。

---

## 4. 解决方案

### 方案 A（推荐）：替换 `Queue` + `listen` 为 `PubSub`

```typescript
// event.ts 修改
function eventResponse(events: EventV2.Interface) {
  return Effect.gen(function* () {
    const instance = yield* InstanceState.context
    const workspaceID = yield* InstanceState.workspaceID

    // 用 PubSub 替代 Queue + listen
    const pubsub = yield* PubSub.unbounded<EventV2.Payload>()
    const unsubscribe = yield* events.listen((event) =>
      PubSub.publish(pubsub, event)  // PubSub.publish 是 Effect，走正规运行时
    )
    yield* Effect.addFinalizer(() => unsubscribe)

    const stream = Stream.fromPubSub(pubsub).pipe(
      // ... 其余不变
    )
    // ...
  })
}
```

**为什么有效**：`PubSub.publish` 是正经 Effect primitive，走 Effect runtime fiber 调度。不依赖 `offerUnsafe` 的 unsafe 路径。

### 方案 B：用 `Stream.fromChannel` 替代 `Stream.fromQueue`

```typescript
const channel = yield* Channel.unbounded<EventV2.Payload>()
const unsubscribe = yield* events.listen((event) =>
  Channel.unsafeOffer(channel, event)
)
const stream = Stream.fromChannel(channel).pipe(...)
```

`Channel` 是 Effect 3.x 的推荐替代方案，其 unsafe 实现比 `Queue.offerUnsafe` 更健壮。

### Bug #2 修复：设置 `keepAliveTimeout`

在 `server.ts:192` 行后添加：

```typescript
const server = createServer()
server.keepAliveTimeout = 0  // SSE 长连接不需要 keepAliveTimeout
```

或缩短 heartbeat 到 3 秒（小于默认 5 秒）作为临时方案。

---

## 5. 总结

| Bug | 位置 | 严重度 | 机制 |
|-----|------|--------|------|
| #1 | `event.ts:72-77` | **高** | `Stream.concat` + `fromQueue` 的 fiber 唤醒在 PID namespace 下失效 |
| #2 | `server.ts:192` | 中 | `keepAliveTimeout` 5s < heartbeat 10s，heartbeat 永久无效 |
| #3 | `event.ts:34-35` | 备选 | `offerUnsafe` 的 unsafe 语义在 PID namespace 下副作用不确定 |

**核心结论**：不是网络层问题，不是 Go runtime 问题——是 Effect 框架的 fiber 唤醒机制在 bwrap PID namespace 下的兼容性问题。修复方向：避免 `Queue.offerUnsafe` + `Stream.fromQueue` 路径，改用 `PubSub` 或 `Channel`。
