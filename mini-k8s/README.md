# mini-k8s

 

一个用 Go 从零实现的本地迷你 Kubernetes，聚焦 **Pod 生命周期管理**。

纯标准库、零外部依赖，用「本地进程」模拟「容器」，可在 macOS/Linux 上直接运行。

 

## 特性

 

- Pod 的创建 / 查询 / 更新 / 删除（REST API + Watch）

- 调度器把未绑定的 Pod 分配到 Ready 节点

- kubelet 驱动容器进程的启动、重启、停止，并回写 Pod 状态

- Pod 生命周期阶段：`Pending → Running → Succeeded / Failed`

- 重启策略：`Always` / `OnFailure` / `Never`（`RestartCount` 正确累加）

- 删除 Pod 后自动清理其残留进程

- 容器日志落盘，可通过 `minictl logs` 查看

- 仿 `kubectl` 的命令行客户端

 

## 架构

 

组件划分仿照真实 Kubernetes，进程间通过 HTTP + Watch 通信：

 

```

                    ┌─────────────┐   REST / Watch   ┌──────────────┐

   minictl ───────► │  apiserver  │ ◄──────────────► │  scheduler   │

                    │ (内存 store) │                   └──────────────┘

                    └──────┬──────┘   REST / Watch

                           │                ┌──────────────┐

                           └──────────────► │   kubelet    │

                                            │ (进程 runtime)│

                                            └──────────────┘

```

 

- **apiserver**：状态中枢，持有内存 store，对外提供 Pod/Node 的 CRUD 与 Watch。

- **scheduler**：周期性扫描 `nodeName` 为空的 Pending Pod，轮询（round-robin）绑定到 Ready 节点。

- **kubelet**：周期性 list 调度到本节点的 Pod，通过进程 runtime 让实际状态向期望状态收敛，并回写 phase。

- **minictl**：命令行客户端，所有操作通过 apiserver 完成。

 

## 目录结构

 

```

cmd/

  mini-k8s/    一体化入口（apiserver+scheduler+kubelet 同进程，一键体验）

  apiserver/   API 服务独立进程

  scheduler/   调度器独立进程

  kubelet/     节点代理独立进程

  minictl/     命令行客户端

pkg/

  apis/core/v1 核心 API 类型

  store        内存存储（CRUD + Watch）

  runtime      进程型容器运行时

  apiserver    HTTP handlers

  client       apiserver 的 Go 客户端

  scheduler    调度逻辑

  kubelet      Pod 生命周期 reconcile

```

 

## 各模块说明

 

### pkg/apis/core/v1 —— 核心类型

定义所有 API 对象，仿照 Kubernetes `core/v1` 精简而来。

 

| 类型 | 说明 |

| --- | --- |

| `TypeMeta` / `ObjectMeta` | 通用的 kind/apiVersion 与元数据（name、namespace、uid、labels、resourceVersion 等） |

| `Pod` / `PodSpec` / `PodStatus` | Pod 的期望状态与实际状态 |

| `Container` / `ContainerStatus` | 容器定义（command/args/env）与运行状态（state/exitCode/restartCount/pid） |

| `PodPhase` | `Pending` / `Running` / `Succeeded` / `Failed` / `Unknown` |

| `RestartPolicy` | `Always` / `OnFailure` / `Never` |

| `Node` / `NodeStatus` | 节点及其心跳状态（`Ready` / `NotReady`） |

| `WatchEvent` | Watch 推送的事件（`ADDED` / `MODIFIED` / `DELETED`） |

| `Pod.DeepCopy()` | 深拷贝，避免 store 内外共享引用导致的数据竞争 |

 

### pkg/store —— 内存存储

充当 apiserver 背后的「etcd」，线程安全（`sync.RWMutex`）。

 

- Pod / Node 的 CRUD，键为 `namespace/name`。

- 每次写操作递增全局 `resourceVersion`。

- 返回的对象均为**深拷贝**，外部修改不会污染内部状态。

- `Watch(resource, sendInitial)`：注册 watcher，返回带缓冲的事件 channel；`sendInitial=true` 时先推送已有对象的 `ADDED` 事件，让客户端获得全量视图再接收增量。

- 广播采用**非阻塞**发送：watcher 消费过慢时丢弃事件，保证 store 不被阻塞（真实 k8s 会返回 `410 Gone` 让客户端重新 list，此处简化）。

 

### pkg/runtime —— 进程型容器运行时

用本地进程模拟容器，是「零依赖本地运行」的关键。

 

- 每个 `Container` 的 `Command[0]` 作为可执行程序，`Command[1:] + Args` 作为参数，`Env` 注入环境变量。

- stdout/stderr 写入日志文件 `<logdir>/<podUID>-<container>.log`。

- `StartContainer`：启动或按重启策略重启进程；`RestartCount` 在重启时累加。

- 后台 goroutine `cmd.Wait()` 等待退出，记录终止状态（exitCode / finishedAt / reason）。

- `StopContainer` / `StopPod` / `StopAll`：通过 `context.CancelFunc` 终止进程。

- `Snapshot` / `PodUIDs` / `HasPod`：供 kubelet 查询容器状态。

 

### pkg/apiserver —— HTTP REST + Watch

基于 Go 1.22 的 `http.ServeMux` 方法+通配符路由。

 

| 方法 | 路径 | 说明 |

| --- | --- | --- |

| GET | `/healthz` | 健康检查 |

| POST | `/api/v1/pods` | 创建 Pod（分配 uid） |

| GET | `/api/v1/pods` | 列出所有 Pod |

| GET | `/api/v1/namespaces/{ns}/pods/{name}` | 获取指定 Pod |

| PUT | `/api/v1/namespaces/{ns}/pods/{name}` | 更新 Pod（以现有对象为基准浅合并） |

| DELETE | `/api/v1/namespaces/{ns}/pods/{name}` | 删除 Pod |

| GET | `/api/v1/watch/pods` | Watch Pod（ndjson 流式推送，含 20s 心跳） |

| POST | `/api/v1/nodes` | 注册/更新节点（刷新心跳） |

| GET | `/api/v1/nodes` | 列出节点 |

| GET | `/api/v1/watch/nodes` | Watch 节点 |

 

### pkg/client —— apiserver Go 客户端

供 scheduler / kubelet / minictl 复用，封装 Pod、Node 的 CRUD 与 `WatchPods`（读取 ndjson 流并解码为类型化事件）。普通请求 10s 超时，watch 请求不设超时。

 

### pkg/scheduler —— 调度器

周期性（默认 2s）执行 `scheduleOnce`：收集 `nodeName` 为空且未进入终态的 Pod，在 Ready 节点间 round-robin 绑定。无 Ready 节点时打印告警并等待。

 

### pkg/kubelet —— 节点代理（生命周期核心）

采用「周期性 list + 全量 reconcile」模型（默认 sync 2s、heartbeat 5s）：

 

1. 注册节点并周期上报 `Ready` 心跳。

2. `syncPod` 逐个收敛容器状态：未创建则启动；已终止则按重启策略决定是否重启。

3. 依据容器状态计算 Pod phase（全部终止→`Succeeded`/`Failed`；有运行→`Running`；否则→`Pending`），构造 `Conditions`。

4. 仅在状态**语义变化**时回写（`statusEqual` 比较），避免写风暴。

5. 清理已从 apiserver 删除、但 runtime 仍持有进程的 Pod。

6. ctx 取消时 `StopAll` 优雅停止本节点所有容器。

 

### cmd/minictl —— 命令行客户端

子命令：`get pods|nodes`、`run`、`apply -f`、`describe pod`、`delete pod`、`logs`。

apiserver 地址通过 `--server` 或环境变量 `MINI_K8S_SERVER` 指定（默认 `http://127.0.0.1:8080`）。

 

> 注意：`run`/`logs` 中 Pod 名是位置参数，须写在 flag 之前；容器命令须放在 `--` 之后。

 

## 构建

 

```bash

cd /Users/sunpeng/go/mini-k8s

 

# 编译全部

go build ./...

 

# 编译到 bin/

go build -o bin/mini-k8s  ./cmd/mini-k8s

go build -o bin/minictl   ./cmd/minictl

go build -o bin/apiserver ./cmd/apiserver

go build -o bin/scheduler ./cmd/scheduler

go build -o bin/kubelet   ./cmd/kubelet

```

 

## 运行

 

### 方式一：一体化（推荐快速体验）

 

```bash

./bin/mini-k8s &            # 默认监听 127.0.0.1:8080

```

 

### 方式二：分进程（体验真实分布式交互）

 

```bash

./bin/apiserver --addr :8080 &

./bin/scheduler --server http://127.0.0.1:8080 &

./bin/kubelet   --server http://127.0.0.1:8080 --node-name node-1 &

```

 

常用 flag：`--logdir`（容器日志目录，默认 `/tmp/mini-k8s/logs`）、`--sync-interval`、`--heartbeat-interval`、`--interval`（scheduler）。

 

## 使用示例

 

```bash

# 创建长驻 Pod（restart Always）

./bin/minictl run web --restart Always -- /bin/sh -c 'echo started; sleep 300'

 

# 创建一次性 Pod（成功退出 / 失败退出）

./bin/minictl run done --restart Never -- /bin/sh -c 'echo hi; exit 0'

./bin/minictl run boom --restart Never -- /bin/sh -c 'echo oops; exit 3'

 

# 带环境变量

./bin/minictl run hello --env NAME=world -- /bin/sh -c 'echo hello $NAME'

 

# 查看与排障

./bin/minictl get pods

./bin/minictl get nodes

./bin/minictl describe pod web

./bin/minictl logs web

./bin/minictl delete pod web

```

 

通过 JSON 清单创建：

 

```bash

cat > pod.json <<'EOF'

{

  "metadata": { "name": "demo", "namespace": "default" },

  "spec": {

    "restartPolicy": "Never",

    "containers": [

      { "name": "c1", "command": ["/bin/sh", "-c", "echo from manifest; exit 0"] }

    ]

  }

}

EOF

./bin/minictl apply -f pod.json

```

 

## 测试

 

### 单元测试

 

```bash

go test ./...          # 全部

go test ./pkg/store/   # 仅 store

go test -v ./pkg/store/ -run TestWatchReceivesEvents

go test -race ./...    # 竞态检测

```

 

当前 `pkg/store/store_test.go` 覆盖：

 

- `TestPodCRUD`：创建/重复创建报错/获取/更新/`resourceVersion` 递增/删除。

- `TestReturnedObjectsAreCopies`：验证 store 返回深拷贝，外部修改不污染内部状态。

- `TestWatchReceivesEvents`：创建 Pod 后 watcher 收到 `ADDED` 事件。

- `TestWatchInitialList`：`sendInitial=true` 时先收到已有对象事件。

- `TestNodeUpsertAndHeartbeat`：节点 upsert 与心跳时间戳。

 

### 端到端手动验证

 

启动服务后，可用下表核对生命周期行为：

 

| 场景 | 命令 | 预期 |

| --- | --- | --- |

| 成功退出 | `run done --restart Never -- /bin/sh -c 'exit 0'` | phase `Succeeded`，`logs` 有输出 |

| 失败退出 | `run boom --restart Never -- /bin/sh -c 'exit 3'` | phase `Failed`，container exitCode=3 |

| 长驻运行 | `run web --restart Always -- /bin/sh -c 'sleep 300'` | phase `Running`，`ps` 可见进程 |

| 崩溃重启 | `run looper --restart Always -- /bin/sh -c 'exit 1'` | `RESTARTS` 随时间递增 |

| 删除清理 | `delete pod web` | Pod 消失，对应进程被 kill（`ps -p <pid>` 查不到） |

 

验证进程清理的脚本片段：

 

```bash

PID=$(./bin/minictl describe pod web | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2)

./bin/minictl delete pod web && sleep 3

ps -p "$PID" >/dev/null && echo "进程仍在（异常）" || echo "进程已清理（正常）"

```

 

## 与真实 Kubernetes 的差异（简化点）

 

- 存储为纯内存，apiserver 重启后状态丢失（未接 etcd）。

- 容器由本地进程模拟，无镜像拉取、无 cgroup/namespace 隔离、无网络与存储卷。

- kubelet 用周期 list 全量 reconcile，未实现 watch + PLEG。

- Watch 对慢消费者丢事件，未实现 `resourceVersion` 断点续传与 `410 Gone` 重连。

- 调度为简单 round-robin，无 Filter/Score、无资源配额。

- 无 Deployment/ReplicaSet/Service 等上层控制器。

 

## 可扩展方向

 

- 用 etcd 替换内存 store 实现持久化。

- 接入 Docker/containerd（CRI）作为真实运行时。

- 增加 Deployment/ReplicaSet 控制器与多容器 Pod 日志聚合。
