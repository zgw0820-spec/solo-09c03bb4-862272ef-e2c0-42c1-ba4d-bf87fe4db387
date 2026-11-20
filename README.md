# 监控数据卷交接与扩容 API

在 Kubernetes 上重建监控库（例如 Prometheus / Thanos）时，旧的持久卷不能随旧
声明一起删掉。值班员需要把这块卷**原样**交给重建后的新 PVC；同时，同一存储类的
监控 PVC 必须**逐块**扩容，避免多块卷同时扩容把存储后端打满。本项目提供：

* 一个 FastAPI 服务，通过 Kubernetes API 完成"卷 → 新声明"的预留与进度查询，
  以及按存储类逐块进行的 PVC 扩容申请；
* 一个本地 Kubernetes API 模拟端，用 Docker Compose 一起启动（两个 API 实例 +
  一个模拟集群），可在 Swagger 里完整走一遍交接与扩容；
* 单元测试、HTTP 端到端测试和并发压测脚本。

```
.
├── app/                 # 交接 / 扩容服务
│   ├── main.py          # FastAPI 路由、错误映射、Swagger
│   ├── service.py       # 交接规则（唯一写入点是 PV 的 spec.claimRef）
│   ├── expansion.py     # 扩容规则、按存储类的共享名额、写入核实状态机
│   ├── kube.py          # 官方 kubernetes 客户端封装 + 错误分类（含"结果未明"）
│   ├── kube_objects.py  # PV/PVC 字段归一化与比较规则
│   ├── quantity.py      # Kubernetes 容量单位解析（Gi/G/M/m…）
│   ├── schemas.py       # 请求/响应模型
│   └── errors.py        # ApiError → JSON
├── mock_kube/           # 本地 Kubernetes API 模拟端
│   ├── app.py           # core/v1 的 PV/PVC/ConfigMap、storage.k8s.io 子集 + 运维夹具
│   ├── store.py         # 内存对象存储、乐观并发、绑定器、扩容驱动模拟
│   └── seed.py          # 演示用预置场景
├── tests/               # pytest
├── scripts/             # demo、双实例扩容演示、并发探针
├── deploy/kubeconfig    # Compose 中服务指向 mock-kube 的 kubeconfig
├── Dockerfile
└── docker-compose.yml
```

## 快速开始

```bash
docker compose up --build
```

* 交接 / 扩容 API 的 Swagger：<http://localhost:8000/docs>（实例 A）
* 第二个 API 实例（与 A 共享同一个模拟集群）：<http://localhost:8002/docs>
* 模拟 Kubernetes API：<http://localhost:8001/docs>（`/_mock/*` 为运维接口）

`mock-kube` 容器以 `MOCK_SEED=1` 启动，预置了演示场景（UID 固定，可直接复制下面的
请求体）。两个 `handover-*` 容器用 `deploy/kubeconfig` 通过官方 Python 客户端访问
`http://mock-kube:8001`，走的是真实 HTTP 路径，而不是进程内假实现。两个实例**不共享
进程内存**：扩容名额保存在集群内的 ConfigMap 中，因此从任一实例提交都看到同一份额度。

### 在 Swagger 里完成一次交接

`POST /handovers`，请求体：

```json
{
  "pv_name": "pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab",
  "old_pvc_uid": "6b1d0f2e-1111-4c2a-9c33-aa11bb22cc33",
  "new_pvc": {
    "namespace": "monitoring",
    "name": "prometheus-data-rebuilt",
    "uid": "9c2e4a71-2222-4d3b-8e55-bb33cc44dd55"
  }
}
```

1. 提交 → `202`，`status: pending_binding`（预留已写入 `spec.claimRef`，尚未 Bound）。
2. 再次提交同一请求 → 仍为 `202`，内容不变；**不会重复写 PV**（幂等重试即查进度）。
3. 让绑定完成 —— 模拟端提供 `POST /_mock/bind/pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab`
   （真实集群里由 PV controller 自动完成）。
4. 再次提交 → `200`，`status: completed`，PV 与 PVC 均已 Bound 且相互指向。
5. 也可以随时只读查询：`GET /handovers/status?pv_name=…&old_pvc_uid=…&namespace=…&name=…&uid=…`

拒绝路径（同样预置好，可直接试）：

| 请求 | 结果 |
| --- | --- |
| 把同一块卷交给 `prometheus-data-rival`（另一个 UID） | `409 pv_reserved_for_other_claim` |
| 交接回收策略为 `Delete` 的卷 | `409 reclaim_policy_not_retain` |
| 卷模式 `Block` 的卷交给 `Filesystem` 的新 PVC | `409 volume_incompatible` |
| 新 PVC 的 `volumeName` 不是这块卷 | `409 volume_incompatible`（`pvc_prebound_to_pv`） |
| 容量/存储类/访问模式不匹配 | `409 volume_incompatible`，`details.checks` 给出逐项结果 |
| 新 PVC 同名重建（UID 变化）后重放旧请求 | `409 new_pvc_uid_mismatch` |

预置数据可用 `POST http://localhost:8001/_mock/reset` 之外的接口自行扩展：
`POST /_mock/pvs`、`POST /_mock/namespaces/{ns}/pvcs`、
`DELETE /_mock/namespaces/{ns}/pvcs/{name}`、`POST /_mock/reconcile`、`GET /_mock/state`。

## 扩容 API（PVC 逐块扩容）

同一存储类的监控 PVC 要**一块一块**扩容：前一笔没有核实完成，后一笔不能开始。
两个服务实例共用名额——名额不放在进程内存，而是放在集群内的协调 ConfigMap
（默认 `monitoring/pvc-expansion-state`）里，靠 `resourceVersion` 做乐观并发，
所以任一实例发起的写入都只有一方能拿到。

### 在 Swagger 里演示名额占用与释放

`POST /expansions`（实例 A，<http://localhost:8000/docs>）：

```json
{
  "request_id": "exp-20261120-prom-001",
  "namespace": "monitoring",
  "name": "prometheus-data-0",
  "uid": "4a1c9e70-aaaa-4bbb-8ccc-00000000000a",
  "target_capacity": "250Gi"
}
```

1. 提交 → `202`，`status: expanding`，`quota.occupied: true`，持有者就是本请求号。
   PVC 的 `spec.resources.requests.storage` 从 `100Gi` 变为 `250Gi`。
2. 用**另一个实例**（<http://localhost:8002/docs>）给同存储类的另一块
   `prometheus-data-1`（请求号 `exp-20261120-prom-002`）提交 → `409
   expansion_in_flight`，`details.holder` 指出占着名额的那一笔。
3. 推进核实：`POST http://localhost:8001/_mock/expansions/reconcile` 模拟存储驱动
   把 `status.capacity.storage` 更新到目标值（真实集群由 CSI 驱动 + PVC controller
   完成）。
4. 用同一请求号再提交 → `200`，`status: completed`，`quota.occupied: false`。
5. 此时实例 B 的 `exp-20261120-prom-002` 再提交才会被受理 → `202`，名额转移到它。
6. 只读查询：`GET /expansions/{request_id}`；名额查询：
   `GET /expansions?storage_class=fast-ssd`（两者都不改变名额）。
7. 同一请求号改参数（例如把 `target_capacity` 改成 `300Gi`）→ `409
   request_id_conflict`，`details.differences` 列出差异。

### 演示"写入成功但回执丢失后重启"

模拟端提供了故障注入接口：

```bash
# 下一次 PVC merge patch 照常生效，但响应以 503 丢弃（回执丢失）
curl -X POST localhost:8001/_mock/faults \
     -H 'Content-Type: application/json' \
     -d '{"drop_pvc_patch_receipts": 1}'
```

然后在实例 A 提交上面的请求体：你会拿到 `202` + `status: unconfirmed`（待核实），
`quota.occupied` 仍为 `true`，`verify_after` 给出可重新核实的时间。此时：

* 实例 B 提交同存储类的另一块 → 仍然 `409 expansion_in_flight`（名额没放）；
* 用同一请求号重提 → 若 PVC 的申请容量已是目标，转为 `expanding`；否则在核实窗口
  内继续 `unconfirmed`；窗口过后仍低于目标，才判定"写入未生效"并接续原申请重写；
* 杀掉实例 A 再启动新进程（Compose 里 `docker compose restart handover-a`）→ 新进程
  没有内存状态，但从 ConfigMap 和 PVC 实时读取后给出相同结论，名额不会被误释放。

`scripts/expansion_demo.py` 会把上面整套流程（含重启、回执丢失、10 路跨实例并发抢
名额）以两个独立进程 + 真实 HTTP 跑一遍并逐条打印。

### 扩容规则

**首次申请（写入之前校验，任一不满足即拒绝且不写集群）**

1. PVC 存在，且集群中该名称的 PVC `metadata.uid` 等于请求里的 UID；
2. PVC `status.phase == Bound`；
3. 目标容量可按 Kubernetes 单位解析、且**严格大于**当前
   `spec.resources.requests.storage`（按字节数值比较，不是字符串）；
4. 存储类存在且 `allowVolumeExpansion: true`。

**名额（每个存储类一笔在途）**

* 名额记录在 ConfigMap 的每个请求号一个键里，`phase` 为 `writing` /
  `unconfirmed` / `expanding` 时占用名额；
* 认领是"读 ConfigMap → 检查同存储类没有其他占用者 → 带 `resourceVersion` 写回"，
  两个实例并发只有一方成功，另一方重新读取后返回 `409 expansion_in_flight`；
* 写入使用 JSON merge patch，并携带 PVC 的 `metadata.uid` 和
  `metadata.resourceVersion` 作前置条件。

**释放名额（只有两种情况）**

1. **核实写入生效**：同一 UID 的 PVC `status.capacity.storage` 达到目标 → `completed`；
2. **核实写入未生效**：`spec.resources.requests.storage` 仍低于目标，且已过核实窗口
   （`EXPANSION_CONFIRM_DELAY_SECONDS`，默认 5s），说明原写入没有落地 → 释放。

**结果未明**：写入超时、连接中断、5xx、或任何无法确认的失败 → `unconfirmed`
（待核实），**继续占用名额**，绝不假设写入失败。只有 API server 明确 4xx 拒绝
（可证明没有写入）才会立即释放。

**请求号**

* 同一请求号、同一组参数（命名空间/名称/UID/目标容量）重提：只查进度，或在核实窗口
  过后接续原申请重新写入，不会重复占用名额；
* 同一请求号、参数不同：`409 request_id_conflict`；
* 服务本身不保存状态，进度全部从 ConfigMap 与实时 PVC 推导，因此可多副本部署、
  随时重启。

**状态码**

| HTTP | status / code | 含义 |
| --- | --- | --- |
| 202 | `expanding` | 申请容量已写入，等待实际容量达到目标（名额仍占用） |
| 202 | `unconfirmed` | 写入结果未明，待核实（名额仍占用），看 `verify_after` |
| 200 | `completed` | 实际容量达到目标，名额已释放 |
| 409 | `expansion_in_flight` | 该存储类已有另一笔扩容占用名额 |
| 409 | `request_id_conflict` | 同一请求号换了参数 |
| 409 | `pvc_not_bound` / `target_not_greater` / `storage_class_*` / `pvc_uid_mismatch` | 准入不通过 |
| 409 | `expansion_write_rejected` | API server 明确拒绝写入，已核实未生效并释放名额 |
| 404 | `pvc_not_found` / `request_not_found` | PVC 或请求号不存在 |
| 422 | `invalid_request` | 请求体或目标容量不合法 |
| 502 | `expansion_not_reflected` | 写入返回成功但对象未反映目标，保持占用待人工核实 |
| 503 | `state_unconfirmed` / `kube_unavailable` | 协调状态或集群访问结果未明 |

扩容相关环境变量：`EXPANSION_STATE_NAMESPACE`（默认 `monitoring`）、
`EXPANSION_STATE_CONFIGMAP`（默认 `pvc-expansion-state`）、
`EXPANSION_CONFIRM_DELAY_SECONDS`（默认 `5`）、`INSTANCE_ID`（记录写操作来自哪个实例）。

## 交接规则

一次交接请求包含：PV 名、旧 PVC 的 UID、新 PVC 的命名空间 / 名称 / UID。

**准入（写之前校验，任一不满足即 409，且不修改集群）**

1. PV 存在，`spec.persistentVolumeReclaimPolicy == Retain`；
2. PV `status.phase == Released`；
3. PV 当前 `spec.claimRef.uid` 等于请求里的旧 PVC UID（即这块卷确实是从旧声明释放的）；
4. 新 PVC 已存在，且集群中该名称的 PVC UID 等于请求里的新 UID；
5. 新 PVC 通过 `spec.volumeName` **预先指定**该卷；
6. 容量：PV `spec.capacity.storage` ≥ 新 PVC `resources.requests.storage`
   （按 Kubernetes 单位解析后做数值比较，不是字符串比较）；
7. 存储类相同；
8. 访问模式：新 PVC 请求的模式是 PV 支持模式的子集（与 PV controller 的
   `IsSubset` 规则一致）；
9. 卷模式相同（`Filesystem` / `Block`）。

**预留**

预留只写一个字段：PV 的 `spec.claimRef` 指向新 PVC 的 `namespace/name/uid`，并附带
两条注解 `handover.example.com/previous-pvc-uid`、`handover.example.com/reserved-at`
便于审计。写入使用 JSON merge patch，并携带 `metadata.resourceVersion` 和
`metadata.uid` 作为前置条件：

* 两个不同接手方并发提交 → API server 只接受一个，另一个拿到 409 后重新读取，
  发现预留已归属他人，返回 `409 pv_reserved_for_other_claim`；
* 同一接手方并发/重复提交 → 冲突后重新读取，发现 `claimRef` 已是自己，按幂等处理，
  只返回进度，不重复写入；
* 服务本身不保存状态，所有进度都从集群实时读取，因此可多副本部署、随时重启。

**状态判定**

* `pending_binding`（HTTP 202）：预留已生效，但 PV 或 PVC 尚未 Bound，或二者没有
  相互指向；`status_text` 会说明卡在哪一侧。
* `completed`（HTTP 200）：PV `status.phase == Bound`、PVC `status.phase == Bound`、
  PV `claimRef` 的 uid 等于 PVC 的 uid、PVC `spec.volumeName` 等于 PV 名。
* 新 PVC 被同名重建（UID 变化）→ 旧请求 409 `new_pvc_uid_mismatch`，不会被误判为
  完成；新 UID 也不能直接抢走旧预留（`409 pv_reserved_for_other_claim`）。

**错误码**

| HTTP | code | 含义 |
| --- | --- | --- |
| 404 | `pv_not_found` | PV 不存在 |
| 409 | `reclaim_policy_not_retain` | 非 Retain |
| 409 | `volume_not_released` | 非 Released / 无 claimRef |
| 409 | `pv_reserved_for_other_claim` | 已预留给别的声明或旧 UID 不符 |
| 409 | `new_pvc_not_found` / `new_pvc_uid_mismatch` | 新 PVC 不存在 / 同名但 UID 不符 |
| 409 | `volume_incompatible` | 容量、存储类、访问模式、卷模式或预绑定不满足 |
| 409 | `no_such_handover` | 只用只读接口查询未发生过的交接 |
| 409 | `concurrent_update` | 持续并发写入导致预留未完成，可重试 |
| 422 | `invalid_request` | 请求体字段非法 |
| 502/503 | `kube_error` / `kube_unavailable` | 集群访问失败 |

## 接入真实集群

服务默认 `KUBE_MODE=remote`，读取 `KUBECONFIG` 或（Pod 内）使用
`KUBERNETES_SERVICE_HOST` 自动切换到 in-cluster 配置。把 `deploy/kubeconfig` 换成
真实集群的配置（或去掉 volume 挂载、改用 ServiceAccount）即可；`KUBE_REQUEST_TIMEOUT`
控制单次调用超时。`KUBE_MODE=mock` 则完全不访问集群，只适合演示。

镜像里只需要 `kubernetes` 客户端的 RBAC：

```yaml
rules:
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "patch"]   # list 仅用于 /healthz 探活；patch 只改 claimRef
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "patch"]           # patch 只改 spec.resources.requests.storage
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "create", "patch"] # 扩容名额协调对象 pvc-expansion-state
  - apiGroups: ["storage.k8s.io"]
    resources: ["storageclasses"]
    verbs: ["get"]                    # 校验 allowVolumeExpansion
```

## 本地运行（不用 Docker）

```bash
python3 -m pip install -r requirements.txt pytest pytest-asyncio httpx

# 终端 1：模拟 Kubernetes API（预置演示数据）
MOCK_SEED=1 uvicorn mock_kube.app:app --port 8001

# 终端 2：交接 / 扩容服务（实例 A）
KUBE_MODE=remote KUBECONFIG=$PWD/deploy/kubeconfig uvicorn app.main:app --port 8000

# 终端 3：第二个实例（可选；与 A 共享同一个模拟集群与名额）
KUBE_MODE=remote KUBECONFIG=$PWD/deploy/kubeconfig INSTANCE_ID=handover-b \
  uvicorn app.main:app --port 8002
```

`deploy/kubeconfig` 里的 server 是 Compose 内部主机名 `mock-kube:8001`；本机直接跑时
改成 `http://127.0.0.1:8001`（或使用 `scripts/demo.py` / `scripts/expansion_demo.py`，
它们会自动生成临时 kubeconfig）。

## 测试与验证

```bash
python3 -m pytest                      # 规则、并发冲突、HTTP 合约、真实客户端到模拟端的端到端
python3 scripts/demo.py                # 按 README 的 7 步走一遍交接，逐条打印
python3 scripts/expansion_demo.py      # 两个真实进程 + mock 集群：回执丢失、重启、名额占用/释放、10 路并发
python3 scripts/concurrency_probe.py   # 20 轮 10 方并发交接 + 12 路重试风暴
```

`tests/test_api.py` 与 `tests/test_expansion_api.py` 的 live 固件真的用 uvicorn 起
模拟端，再由官方 `kubernetes` 客户端通过 HTTP 访问，覆盖 merge patch 的
Content-Type、`Status` 错误体解析、404/409 分类、以及"写入生效但回执丢失 + 重启"
的真实链路。

## 已知限制

* 模拟端只实现本服务用到的 `core/v1` 与 `storage.k8s.io/v1` 子集（PV/PVC/ConfigMap
  读写、StorageClass 读、PVC 扩容 patch、夹具接口），不是通用 Kubernetes 模拟器；
  它不会运行 scheduler、kubelet、存储驱动，绑定需要 `POST /_mock/bind/...`，
  实际容量增长需要 `POST /_mock/expansions/reconcile`。
* 只处理 `PersistentVolume` 已处于 `Released` 的场景。旧 PVC 仍存在（`Bound`）时
  必须先删除旧工作负载和旧 PVC，本服务不会代劳删除。
* 没有对 `ReadWriteOncePod` 等特殊访问模式做额外校验，按子集规则统一处理。
* 扩容名额按存储类串行，是**协作式**的：它只约束经本接口发起的扩容。绕过本接口直接
  patch PVC、或存储类被其他系统扩容，本服务无从阻止。协调 ConfigMap 不是事务日志，
  若人工删除它，正在核实的记录会丢失，需要按 PVC 实际状态人工判断。
* 核实窗口（`EXPANSION_CONFIRM_DELAY_SECONDS`）是一个经验值：必须覆盖 API server /
  代理的最大写入延迟，否则极端情况下可能过早判定"未生效"。真实集群建议配合
  `status.capacity` 与存储驱动事件使用。
* 未实现多集群、限流与审计落库；注解与 ConfigMap 记录只用于人工排查。
