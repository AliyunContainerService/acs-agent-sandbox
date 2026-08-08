# Sandbox 升级使用STS挂载OSS操作手册 (SOP)

## 一、适用场景

对已分配的使用AKSK方式挂载Oss的沙箱， 执行快照重建升级， 升级后改为使用STS方式挂载oss：创建快照 → kill 原实例 → 从快照克隆同名新实例。

## 二、前置准备

### 2.1 环境

- Python 3.8+，已安装 `e2b`、`e2b_code_interpreter` SDK 和 `kubernetes` Python 包（`pip install kubernetes`）
- kubeconfig 可访问目标集群
- sandbox-manager 已部署，域名可 DNS 解析

### 2.2 需要准备的信息

| 参数 | 说明                                                        | 示例                                                      |
|------|-----------------------------------------------------------|---------------------------------------------------------|
| kubeconfig | 集群认证文件路径                                                  | `/path/to/kubeconfig`                                   |
| E2B_DOMAIN | sandbox-manager 域名                                        | `e2b-staging.example.com`                               |
| E2B_API_KEY | E2B API Key, 需要和创建sandbox使用相同的KEY                         | `sk-staging-abc123def456`                               |
| Python 路径 | 安装了 e2b SDK 和 kubernetes 包的 Python 解释器                    | `/opt/venv/bin/python3`                                 |
| 脚本目录 | upgrade_sts.py 所在目录                                       | `/path/to/sbx/`                                         |
| OSS 新老PV 的映射 | 对每个原来使用AKSK方式挂载的PV， 说明对应的使用STS的PV                         | `oss-aksk-pv-1:oss-sts-pv-1;oss-aksk-pv-2:oss-sts-pv-2` |
| 默认的CredientalProvider | 用于配置PV挂载所需要的CredientalProvider 名， 读写和只读用`:`分割， 只读可省略默认同读写 | `oss-rw:oss-ro` 或 `oss-rw`                              | 
| Agent应用名 | 沙箱运行的agent的应用名                                            | `openclaw`                                              |

### 2.3 需要准备的信息

### 获取目标 sandbox 名称（单个升级用）

```bash
kubectl --kubeconfig /path/to/kubeconfig get sbx -n default \
  -l agents.kruise.io/sandbox-claimed=true
```

### 检查PV

```bash
kubectl --kubeconfig /path/to/kubeconfig get pv
```
找到对应使用AKSK的pv，以及使用STS的pv

### 检查CredientalProvider

```bash
kubectl --kubeconfig /path/to/kubeconfig get CredientalProvider
```
找到对应描述readwrite和readonly 的CredientalProvider名字

### 检查AgentIdentity

```bash
kubectl --kubeconfig /path/to/kubeconfig get AgentIdentity
```
找到对应需要填入`security.agents.kruise.io/agent-name`的AgentIdentity


---

## 三、单个升级

1. 对涉及沙箱的所需使用AKSK的PV， 都新建一个配置使用STS的PV
2. 针对读写和只读分别创建两个CredientalProvider， 作为后续STS token的权限设置模版
3. 执行单个升级的脚本， 并传入新老pv的映射关系、CredentialProvider名以及Agent应用名

### 3.0 前置检查（脚本自动执行）

脚本在 Step 1 之前会对沙箱做以下检查：

1. **E2B 归属检查**：沙箱无 `agents.kruise.io/owner` annotation 时跳过升级（非 E2B 创建）
2. **已升级检查**：沙箱已有 `security.agents.kruise.io/agent-name` annotation 时：
   - 若同时有 `security.agents.kruise.io/storage-auth` annotation 但 `spec.runtimes` 中**缺少** `csi` 和 `agent-runtime`，则判定为**部分升级**（sidecar 可能异常），脚本自动执行回滚后继续正常升级流程：
     - 反向转换 `csi-volume-config` annotation：移除 `credentialProviderName` 属性，PV 名映射回老 PV（多个老 PV 映射到同一新 PV 时，选取 creationTimestamp 最早的）
     - 删除 `security.agents.kruise.io/storage-auth`、`security.agents.kruise.io/agent-name`、`security.agents.kruise.io/token-status` 三个 annotation
   - 否则判定为已完成升级，直接跳过

### 3.1 命令格式

```bash
E2B_DOMAIN=<域名> E2B_API_KEY=<密钥> python3 upgrade_sts.py \
  --name <sandbox名称> \
  -n <命名空间> \
  --pv-map <新老PV映射> \
  --default-cred <CredentialProvider名> \
  --agent-name <Agent应用名> \
  [--kubeconfig <kubeconfig路径>] \
  [--timeout <超时秒数>] \
  [--gc-checkpoint]
```

| 参数 | 位置 | 必传 | 默认值 | 说明                                                             |
|------|------|------|--------|----------------------------------------------------------------|
| E2B_DOMAIN | 环境变量 | 是 | - | sandbox-manager 域名                                             |
| E2B_API_KEY | 环境变量 | 是 | - | E2B API Key                                                    |
| `--name` | 命令行 | 是 | - | Sandbox 的 metadata.name                                        |
| `-n`, `--namespace` | 命令行 | 否 | `default` | Sandbox 所在命名空间                                                 |
| `--pv-map` | 命令行 | 是 | - | 新老PV映射，格式 `oss-aksk-pv-1:oss-sts-pv-1;oss-aksk-pv-2:oss-sts-pv-2`                      |
| `--default-cred` | 命令行 | 是 | - | 默认CredentialProvider名，格式 `oss-rw` 或 `oss-rw:oss-ro`；省略ro时默认同rw |
| `--agent-name` | 命令行 | 是 | - | Agent应用名，写入 `security.agents.kruise.io/agent-name` annotation  |
| `--kubeconfig` | 命令行 | 否 | 默认kubeconfig | kubeconfig文件路径，用于通过 Kubernetes Python 客户端读取沙箱的CSI挂载配置                       |
| `--timeout` | 命令行 | 否 | 保留原策略 | 沙箱超时时间（秒）；不指定时自动保留原沙箱策略：有 pauseTime 则按 pauseTime 剩余时间设为 auto-pause，有 shutdownTime 则按剩余时间设为超时，均无则设为 never-timeout |
| `--gc-checkpoint` | 命令行 | 否 | `false` | 是否在升级完成后删除中间 checkpoint。不指定时保留 checkpoint 以备手动恢复；指定后执行 Step 8 删除 |

### 3.2 执行示例

```bash
cd /path/to/sbx/

# 示例 1：default 命名空间，指定读写和只读CredentialProvider
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 upgrade_sts.py \
  --name code-interpreter-prod-x7k2m \
  --pv-map oss-aksk-pv-1:oss-sts-pv-1;oss-aksk-pv-2:oss-sts-pv-2 \
  --default-cred oss-rw:oss-ro \
  --agent-name openclaw

# 示例 2：指定命名空间和超时时间（300秒）
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 upgrade_sts.py \
  --name code-interpreter-prod-x7k2m \
  -n sandbox-ns \
  --pv-map oss-aksk-pv-1:oss-sts-pv-1 \
  --default-cred oss-rw \
  --agent-name openclaw \
  --timeout 300

# 示例 3：升级后自动清理 checkpoint
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 upgrade_sts.py \
  --name code-interpreter-prod-x7k2m \
  --pv-map oss-aksk-pv-1:oss-sts-pv-1 \
  --default-cred oss-rw:oss-ro \
  --agent-name openclaw \
  --gc-checkpoint
```

### 3.3 预期输出

**场景 1：原沙箱非休眠状态**

```
[1] Reading CSI volume config from sandbox CR...
    Original sandbox has no ShutdownTime (never-timeout)
    Transformed CSI config: [{"pvName": "oss-sts-pv-1", "mountPath": "/oss-data/sub1", "subPath": "read-01", "readOnly": true, "attributes": {"credentialProviderName": "oss-ro"}}, {"pvName": "oss-sts-pv-2", "mountPath": "/oss-data/sub3", "subPath": "read-write-01", "attributes": {"credentialProviderName": "oss-rw"}}]
[2] Connecting to sandbox: default--code-interpreter-ossfs-agent-identity-fs86f
    Connected. sandbox id: default--code-interpreter-ossfs-agent-identity-fs86f
[3] Checking DNS policy...
    DNS policy is already ClusterFirst
    Adding runtime 'csi' to spec.runtimes
    Adding runtime 'agent-runtime' to spec.runtimes
    Removing legacy initContainers: init, csi-sidecar, csi-agent-sidecar
    Removing legacy volumes: envd-volume, token-volume
    Removing postStart hook from container 'sandbox'
    Removing volumeMounts from container 'sandbox': envd-volume, token-volume
    Removing env vars from container 'sandbox': ENVD_DIR, POD_UID, GODEBUG
[4] Creating snapshot...
    Snapshot created: cp-bp100987kj45vjtbrs0c
    If upgrade fails after this point, restore with:
    python3 restore_from_cp.py --checkpoint cp-bp100987kj45vjtbrs0c -n default
[5] Killing original sandbox: default--code-interpreter-ossfs-agent-identity-fs86f
    Kill request sent, waiting for sandbox to be fully removed...
    Sandbox fully removed.
[6] Creating new sandbox from snapshot 'cp-bp100987kj45vjtbrs0c' with name 'code-interpreter-ossfs-agent-identity-fs86f'...
    New sandbox created: default--code-interpreter-ossfs-agent-identity-fs86f
    Done!
[7] Skipping re-pause (original was not paused)
[8] Skipping checkpoint deletion (--gc-checkpoint not set): cp-bp100987kj45vjtbrs0c
```

**场景 2：原沙箱处于休眠状态**

```
[1] Reading CSI volume config from sandbox CR...
    Original sandbox ShutdownTime: 2026-07-15T10:00:00Z
    Transformed CSI config: [{"pvName": "oss-sts-pv-1", "mountPath": "/oss-data/sub1", "subPath": "read-01", "readOnly": true, "attributes": {"credentialProviderName": "oss-ro"}}]
[2] Connecting to sandbox: default--code-interpreter-ossfs-agent-identity-fs86f
    Original sandbox is paused, will re-pause after upgrade
    Connected. sandbox id: default--code-interpreter-ossfs-agent-identity-fs86f
[3] Checking DNS policy...
    DNS policy is already ClusterFirst
    spec.runtimes already includes csi and agent-runtime
    No legacy sidecar initContainers to remove
    No legacy sidecar volumes to remove
    No legacy postStart hooks to remove
    No legacy sidecar volumeMounts to remove
    No legacy runtime env vars to remove
[4] Creating snapshot...
    Snapshot created: cp-bp100987kj45vjtbrs0c
    If upgrade fails after this point, restore with:
    python3 restore_from_cp.py --checkpoint cp-bp100987kj45vjtbrs0c -n default --shutdown-time 2026-07-15T10:00:00Z
[5] Killing original sandbox: default--code-interpreter-ossfs-agent-identity-fs86f
    Kill request sent, waiting for sandbox to be fully removed...
    Sandbox fully removed.
[6] Creating new sandbox from snapshot 'cp-bp100987kj45vjtbrs0c' with name 'code-interpreter-ossfs-agent-identity-fs86f'...
    New sandbox created: default--code-interpreter-ossfs-agent-identity-fs86f
    Done!
[7] Re-pausing sandbox (original was paused)...
    Pause request sent, waiting for sandbox to be paused...
    Sandbox paused.
[8] Skipping checkpoint deletion (--gc-checkpoint not set): cp-bp100987kj45vjtbrs0c
```

### 3.4 失败处理

脚本中途失败不会自动回滚，sandbox 保持当前状态。根据输出中 `[N]` 步骤号定位失败原因：

| 失败步骤 | 可能原因 | 处理方式 |
|----------|----------|----------|
| [1] Read CSI Config | Kubernetes API 调用失败或无 CSI 配置 | 确认 kubeconfig 路径；若无 CSI 配置脚本会直接退出 |
| [2] Connect | sandbox 不存在或域名不可达 | 确认 sandbox 名称和 E2B_DOMAIN |
| [3] Spec Fix | Kubernetes API get/patch 失败 | 确认 kubeconfig 权限；可手动 `kubectl patch sbx <name> -n <ns> --type=merge` 修正 dnsPolicy 和 runtimes |
| [4] Snapshot | checkpoint 超时或服务端异常 | 重试，或检查 sandbox-manager 日志 |
| [5] Kill | sandbox 删除超时 | 手动 `kubectl delete sbx <name> -n <ns>` 后重试 |
| [6] Recreate | 409（旧 CR 未清理完）或 504（ALB 超时） | 脚本已内置重试；若仍失败，等待 1 分钟后重跑 |
| [7] Re-pause | SDK pause 调用失败或等待超时 | 手动通过 kubectl `patch sbx <name> --type=merge -p '{"spec":{"paused":true}}'` 暂停 |
| [8] Delete Checkpoint | SDK `delete_snapshot` 调用失败 | 仅在指定 `--gc-checkpoint` 时执行；失败时打印错误并退出，可手动调用 `DELETE /templates/{snapshot_id}` 清理 |

---

## 四、从 Checkpoint 恢复沙箱

当 `upgrade_sts.py` 在创建快照（Step 4）之后的步骤失败时（如 kill、clone、re-pause 等），可以使用已创建的 checkpoint 通过 `restore_from_cp.py` 恢复沙箱。`upgrade_sts.py` 在创建快照后会自动打印恢复命令。

### 4.1 前置条件

- `upgrade_sts.py` 已成功执行到 Step 4（创建快照），输出中包含 `Snapshot created: <checkpoint_id>`
- 原沙箱已不存在（被 kill 或自然消亡）
- checkpoint 未被删除（Step 8 未执行或执行失败）

### 4.2 命令格式

```bash
E2B_DOMAIN=<域名> E2B_API_KEY=<密钥> python3 restore_from_cp.py \
  [--checkpoint <checkpoint_id>] \
  [--checkpoint-cr <namespace/name>] \
  -n <命名空间> \
  [--kubeconfig <kubeconfig路径>] \
  [--pause-time <metav1.Time格式>] \
  [--shutdown-time <metav1.Time格式>]
```

| 参数 | 位置 | 必传 | 默认值 | 说明 |
|------|------|------|--------|------|
| E2B_DOMAIN | 环境变量 | 是 | - | sandbox-manager 域名 |
| E2B_API_KEY | 环境变量 | 是 | - | E2B API Key |
| `--checkpoint` | 命令行 | 与 `--checkpoint-cr` 二选一 | - | Checkpoint ID（`upgrade_sts.py` 输出的 snapshot_id），在命名空间内遍历匹配 `status.checkpointId` |
| `--checkpoint-cr` | 命令行 | 与 `--checkpoint` 二选一 | - | Checkpoint CR 引用，格式 `namespace/name`，直接按 namespace/name 定位 Checkpoint CR（checkpoint ID 从其 `status.checkpointId` 读取）；两者同时指定时按此直接定位并校验 ID 一致性 |
| `-n`, `--namespace` | 命令行 | 否 | `default` | Checkpoint 所在命名空间（仅 `--checkpoint` 方式使用；`--checkpoint-cr` 方式以其 namespace 为准） |
| `--kubeconfig` | 命令行 | 否 | 默认kubeconfig | kubeconfig文件路径，用于通过 Kubernetes Python 客户端查询 Checkpoint CR |
| `--pause-time` | 命令行 | 否 | - | 绝对暂停时间，metav1.Time 格式（如 `2026-07-23T16:07:44Z`）。设置后沙箱将在该时间自动暂停 |
| `--shutdown-time` | 命令行 | 否 | - | 绝对关闭时间，metav1.Time 格式（如 `2026-07-23T16:07:44Z`）。设置后沙箱将在该时间自动关闭 |

> `--pause-time` 和 `--shutdown-time` 互斥，不能同时指定。若都不指定，则设为 never-timeout。

> 两种定位方式的获取途径：
> - **Checkpoint ID**（`--checkpoint`）可直接从 `upgrade_sts.py` 的输出中获取（`Snapshot created: <checkpoint_id>` 行，升级脚本也会自动打印带此 ID 的恢复命令），是升级失败后恢复的首选方式
> - **Checkpoint CR 引用**（`--checkpoint-cr`）不会出现在 `upgrade_sts.py` 的输出中，需通过其他方式查询获得，例如按 checkpoint ID 检索 CR：
>
>   ```bash
>   kubectl --kubeconfig /path/to/kubeconfig get checkpoints -n <命名空间> \
>     -o jsonpath='{range .items[?(@.status.checkpointId=="<checkpoint_id>")]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}'
>   ```
>
>   或直接浏览列表找到目标 CR：`kubectl get checkpoints -n <命名空间>`。适用于升级输出已丢失、或命名空间内 Checkpoint 较多需精确定位的场景

### 4.3 执行示例

```bash
# 示例 1：使用 upgrade_sts.py 输出的恢复命令（never-timeout 沙箱）
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 restore_from_cp.py \
  --checkpoint cp-bp100987kj45vjtbrs0c \
  -n default

# 示例 2：指定 pauseTime（原沙箱有 pauseTime）
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 restore_from_cp.py \
  --checkpoint cp-bp100987kj45vjtbrs0c \
  -n default \
  --pause-time 2026-07-23T16:07:44Z

# 示例 3：指定 shutdownTime（原沙箱有 shutdownTime 但无 pauseTime）
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 restore_from_cp.py \
  --checkpoint cp-bp100987kj45vjtbrs0c \
  -n default \
  --shutdown-time 2026-07-25T10:00:00Z

# 示例 4：按 namespace/name 直接定位 Checkpoint CR（无需 checkpoint ID）
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 restore_from_cp.py \
  --checkpoint-cr default/cp-bp100987kj45vjtbrs0c-x7k2m
```

### 4.4 预期输出

**场景 1：沙箱不存在，从 checkpoint 恢复**

```
[1] Looking up checkpoint: cp-bp100987kj45vjtbrs0c in namespace default
    Found checkpoint: sandbox_name=code-interpreter-ossfs-agent-identity-fs86f
[2] Checking if sandbox already exists: default--code-interpreter-ossfs-agent-identity-fs86f
    Sandbox not found, proceeding to restore.
[3] Cloning sandbox from checkpoint 'cp-bp100987kj45vjtbrs0c' with name 'code-interpreter-ossfs-agent-identity-fs86f'...
    Using never-timeout
    New sandbox created: default--code-interpreter-ossfs-agent-identity-fs86f
    Done!
```

**场景 2：沙箱已存在，直接退出**

```
[1] Looking up checkpoint: cp-bp100987kj45vjtbrs0c in namespace default
    Found checkpoint: sandbox_name=code-interpreter-ossfs-agent-identity-fs86f
[2] Checking if sandbox already exists: default--code-interpreter-ossfs-agent-identity-fs86f
    Sandbox already exists (state: SandboxState.RUNNING), nothing to do.
```

### 4.5 脚本流程

1. **Step 1**: 通过 Kubernetes API 查询 Checkpoint CR：指定 `--checkpoint-cr` 时直接按 namespace/name 获取；否则在命名空间内遍历按 `status.checkpointId` 匹配。获取 `spec.podName` 作为沙箱名称
2. **Step 2**: 通过 E2B SDK `get_info()` 检查沙箱是否已存在
   - 若沙箱处于 Running/Paused 状态：直接退出（无需恢复）
   - 若沙箱处于 dead 等不可读状态：报错退出（需手动清理）
   - 若沙箱不存在（SandboxNotFoundException）：继续恢复
3. **Step 3**: 从 checkpoint 克隆新沙箱，根据 `--pause-time`/`--shutdown-time` 设置超时策略

### 4.6 失败处理

| 失败步骤 | 可能原因 | 处理方式 |
|----------|----------|----------|
| [1] Lookup Checkpoint | Checkpoint CR 不存在或 Kubernetes API 调用失败 | 确认 checkpoint ID（或 `--checkpoint-cr` 的 namespace/name）；检查 kubeconfig 路径 |
| [2] Check Sandbox | 沙箱处于 dead 状态 | 手动 `kubectl delete sbx <name> -n <ns>` 后重试 |
| [3] Clone | 409（旧 CR 未清理完）或 504（ALB 超时） | 脚本已内置重试；若仍失败，等待 1 分钟后重跑 |

---

## 五、批量升级

使用 `batch_upgrade_sts.py` 批量升级多个沙箱。脚本在进程内直接调用 `upgrade_sts.py` 的 `main()` 函数（不另起 Python 进程），支持并发执行、失败阈值熔断和 SIGTERM 中断。

### 5.1 命令格式

```bash
E2B_DOMAIN=<域名> E2B_API_KEY=<密钥> python3 batch_upgrade_sts.py \
  -f <沙箱列表文件> \
  [-c <配置文件>] \
  [-p <并发度>] \
  [-m <最大失败数>]
```

| 参数 | 必传 | 默认值 | 说明 |
|------|------|--------|------|
| `-f`, `--file` | 是 | - | 沙箱列表文件，每行一个沙箱名称（不含命名空间前缀）；空行和 `#` 开头的注释行跳过；格式错误的行（如含 `/`）记录为 FAILED 但不中断批量；输出文件路径由该文件名派生，不可另行指定：状态文件 `<列表文件>.status`、详细日志文件 `<列表文件>.log` |
| `-c`, `--config` | 否 | `sts.conf` | 升级配置文件，每行 `key=value`，可识别的 key：`namespace`（作为 `-n` 传给 upgrade_sts.main，必填）、`pv-map`（`--pv-map`）、`default-cred`（`--default-cred`）、`agent-name`（`--agent-name`）、`kubeconfig`（`--kubeconfig`）、`timeout`（`--timeout`，整数）、`gc-checkpoint`（`--gc-checkpoint`，`true`/`false`）；仅 `namespace` 为必填，其余 key 省略时以空值透传给 upgrade_sts.main（若其内部依赖该值，对应沙箱会逐个失败）；所有升级设置均由该文件提供，脚本无对应 CLI 选项；文件不存在时报错退出 |
| `-p`, `--parallelism` | 否 | `1` | 任意时刻并发执行的升级沙箱数量 |
| `-m`, `--max-failure` | 否 | 无限制 | FAILED 数超过阈值后中断整体升级：不再启动新升级（未启动的记为 ABORTED），已在升级中的正常完成；SKIPPED 不计入 |

输出文件说明：

- **状态文件** `<列表文件>.status`：每个沙箱一行简要结果
- **详细日志文件** `<列表文件>.log`：每个沙箱的完整输出（stdout 与 stderr，stderr 以 `--- stderr ---` 标记分隔）连续输出为一个块，不会交错；每个块内记录该沙箱升级操作的 `[start]` 开始时间、`[end]` 结束时间及耗时；worker 的 stderr 不会打印到控制台。例如 `-f sandbox_list.txt` 对应 `sandbox_list.txt.status` 和 `sandbox_list.txt.log`；两个文件均为追加模式，重复执行同一列表文件时结果累积

配置文件 `sts.conf` 示例：

```
# 升级配置
namespace=default
pv-map=oss-aksk-pv-1:oss-sts-pv-1
default-cred=oss-rw:oss-ro
agent-name=openclaw
kubeconfig=/path/to/kubeconfig
timeout=3600
gc-checkpoint=false
```

### 5.2 执行前预校验

批量升级开始前，脚本会用配置文件中的 `kubeconfig`（未配置时用默认 kubeconfig）对升级配置做集群侧预校验，任一项不通过即报错退出（退出码 `1`），不启动任何升级：

| 校验项 | 校验内容 |
|--------|----------|
| `pv-map` | 每个目的 PV 存在，且 `spec.volumeAttributes.authType`（或 `spec.csi.volumeAttributes.authType`）为 `agent-identity` |
| `default-cred` | 每个 CredentialProvider（rw 与 ro）存在，且 `spec.type` 为 `RAM` |
| `agent-name` | 对应的 AgentIdentity 存在 |

CredentialProvider / AgentIdentity 属于 `agentidentity.alibabacloud.com/v1alpha1`，脚本通过 API discovery 定位其 group（优先 `agentidentity.alibabacloud.com`，discovery 时自动跳过无响应的坏 group），无需硬编码。配置文件中未设置的 key 跳过对应校验。

### 5.3 沙箱列表文件示例

每行一个沙箱名称（命名空间统一由配置文件的 `namespace` 决定）：

```
# 生产集群待升级沙箱
code-interpreter-prod-x7k2m
code-interpreter-prod-a1b2c
openclaw-sbx-01
```

### 5.4 执行示例

```bash
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 batch_upgrade_sts.py \
  -f sandbox_list.txt \
  -c sts.conf \
  -p 4 \
  -m 5
```

### 5.5 状态文件格式

每个沙箱完成时写入一行，结尾附汇总行：

```
default/code-interpreter-prod-x7k2m  SUCCESS
default/code-interpreter-prod-a1b2c  SKIPPED  Sandbox default/code-interpreter-prod-a1b2c already upgraded, skipping
sandbox-ns/openclaw-sbx-01  FAILED  Failed to create snapshot: ...
ns/unstarted-sbx  ABORTED  batch aborted after 6 failures
ns/inflight-sbx  INTERRUPTED  received SIGTERM
# total=5 success=1 skipped=1 failed=1 aborted=1 interrupted=1
```

| 状态 | 含义 |
|------|------|
| SUCCESS | 升级成功 |
| SKIPPED | 前置检查跳过（已升级、非 E2B 创建、无 CSI 配置等） |
| FAILED | 升级失败，附错误信息 |
| ABORTED | 因 `-m` 阈值熔断或收到信号，未启动升级 |
| INTERRUPTED | 收到 SIGTERM/SIGINT，升级被立即中断 |

### 5.5 信号处理与退出码

- 收到 **SIGTERM/SIGINT** 后：不再启动新升级；对升级中的沙箱立即中断并记为 INTERRUPTED（已产生的部分输出仍写入日志文件）；被中断的沙箱可能停留在升级中间态（如已建 snapshot 未 clone），需结合日志人工确认或用 `restore_from_cp.py` 恢复
- 退出码：`0` 全部成功/跳过；`1` 存在 FAILED；`2` 因 `-m` 熔断；`130` 收到 SIGTERM/SIGINT

---

## 六、注意事项

1. **sandbox 升级后标签会变**：快照重建后 `sandbox-template` 标签会带 hash 后缀，`sandbox-pool` 标签可能丢失。

2. **Python 依赖**：执行脚本的 Python 解释器必须已安装 `e2b`、`e2b_code_interpreter` 和 `kubernetes` 包，建议使用虚拟环境路径。`kubernetes` 包可通过 `pip install kubernetes` 安装。

3. **超时策略保留**：不指定 `--timeout` 时，脚本自动检测原沙箱的 pauseTime 和 ShutdownTime：
   - 有 pauseTime：设为 auto-pause，超时时间为 pauseTime 剩余时间（最少 600 秒），服务端超时后自动暂停
   - 有 ShutdownTime：按 ShutdownTime 剩余时间设为超时（最少 600 秒）
   - 均无：设为 never-timeout
   指定 `--timeout` 则覆盖上述策略。当原沙箱有 pauseTime 时，`--timeout` 仅覆盖超时秒数，auto-pause 行为不变。

4. **休眠状态感知**：脚本在连接沙箱前（Step 2）通过 E2B SDK `get_info()` 检测原沙箱是否处于休眠状态。若原沙箱休眠中且原沙箱有 pauseTime，则创建时设置 `lifecycle={'on_timeout': 'pause'}` 由服务端自动暂停；若原沙箱休眠中但无 pauseTime，则升级完成后通过 SDK `pause()` 手动暂停。若原沙箱非休眠状态，跳过此步骤。

5. **CSI 配置自动转换**：脚本优先读取 `csi-volume-config` 注解（多卷挂载格式）。若该注解不存在，脚本会回退检查旧版单卷挂载注解（`csi-volume-name`、`csi-mount-point`、`csi-subpath`），并自动转换为多卷挂载格式（`readOnly` 默认为 `false`，即读写挂载，因为旧版注解无 `readOnly` 字段）。若两种格式均不存在，脚本在 Step 1 直接退出，不会连接沙箱。在 `prepare_fix_sts.py` 中，转换后还会自动清除旧版单卷注解。

6. **失败不回滚**：任何步骤失败后该 sandbox 保持当前状态，不会自动恢复。需根据脚本输出中 `[N]` 步骤号定位失败原因，手动处理后重跑。

7. **中途 Checkpoint 清理**：升级流程最后一步（Step 8），仅在指定 `--gc-checkpoint` 时执行，通过 E2B SDK `Sandbox.delete_snapshot()` 删除快照产生的 Checkpoint。默认不删除，保留 checkpoint 以备后续通过 `restore_from_cp.py` 手动恢复。

8. **Spec 检查与修正**：创建快照前（Step 3），脚本检查并一次性 patch 修正 Sandbox CR 的以下内容（不等待 Pod 生效，直接继续后续步骤）：
   - `dnsPolicy` 非 `ClusterFirst` 时修正为 `ClusterFirst`，并清除 `upgradePolicy`
   - `spec.runtimes` 中补齐 `csi` 和 `agent-runtime`（保留 `traffic-proxy` 等已有配置）
   - 移除遗留 initContainers：`init`、`csi-sidecar`、`csi-agent-sidecar`（改由 runtimes 机制注入）
   - 移除遗留 volumes 及容器中对应的 volumeMounts：`envd-volume`、`fuse-device`、`mount-root`、`nas-plugin-dir`、`oss-plugin-dir`、`run-cnfs`、`efc-metrics-dir`、`ossfs-metrics-dir`、`csi-agent-config`、`token-volume`
   - 移除非 init 容器中执行 `/mnt/envd/envd-run.sh` 的 postStart hook
   - 移除非 init 容器中的 `ENVD_DIR`、`POD_UID`、`GODEBUG` 环境变量
