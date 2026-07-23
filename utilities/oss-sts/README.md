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
  [--pause-after <延迟暂停秒数>]
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
| `--timeout` | 命令行 | 否 | 保留原策略 | 沙箱超时时间（秒）；不指定时自动保留原沙箱策略：原沙箱无 ShutdownTime 则设为 never-timeout |
| `--pause-after` | 命令行 | 否 | `0` | 升级后自动暂停超时秒数；仅原沙箱休眠时生效。0 表示立即通过 SDK 暂停；N > 0 时创建时设置 `lifecycle={'on_timeout':'pause'}`，服务端 N 秒后自动暂停 |

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

# 示例 3：原沙箱休眠中，升级后等待60秒再暂停
E2B_DOMAIN=e2b-staging.example.com \
E2B_API_KEY=sk-staging-abc123def456 \
/opt/venv/bin/python3 upgrade_sts.py \
  --name code-interpreter-prod-x7k2m \
  --pv-map oss-aksk-pv-1:oss-sts-pv-1 \
  --default-cred oss-rw:oss-ro \
  --agent-name openclaw \
  --pause-after 60
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
[8] Deleting intermediate checkpoint: cp-bp100987kj45vjtbrs0c
    Checkpoint deleted: cp-bp100987kj45vjtbrs0c
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
[8] Deleting intermediate checkpoint: cp-bp100987kj45vjtbrs0c
    Checkpoint deleted: cp-bp100987kj45vjtbrs0c
```

### 3.4 失败处理

脚本中途失败不会自动回滚，sandbox 保持当前状态。根据输出中 `[N]` 步骤号定位失败原因：

| 失败步骤 | 可能原因 | 处理方式 |
|----------|----------|----------|
| [1] Read CSI Config | Kubernetes API 调用失败或无 CSI 配置 | 确认 kubeconfig 路径；若无 CSI 配置脚本会直接退出 |
| [2] Connect | sandbox 不存在或域名不可达 | 确认 sandbox 名称和 E2B_DOMAIN |
| [3] DNS Policy | Kubernetes API get/patch 失败 | 脚本打印警告继续执行；可手动 `kubectl patch sbx <name> -n <ns> --type=merge -p '{"spec":{"template":{"spec":{"dnsPolicy":"ClusterFirst"}}}}'` |
| [4] Snapshot | checkpoint 超时或服务端异常 | 重试，或检查 sandbox-manager 日志 |
| [5] Kill | sandbox 删除超时 | 手动 `kubectl delete sbx <name> -n <ns>` 后重试 |
| [6] Recreate | 409（旧 CR 未清理完）或 504（ALB 超时） | 脚本已内置重试；若仍失败，等待 1 分钟后重跑 |
| [7] Re-pause | SDK pause 调用失败或等待超时 | 手动通过 kubectl `patch sbx <name> --type=merge -p '{"spec":{"paused":true}}'` 暂停 |
| [8] Delete Checkpoint | SDK `delete_snapshot` 调用失败 | 脚本仅打印警告不中断；可手动调用 `DELETE /templates/{snapshot_id}` 清理 |

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
  --checkpoint <checkpoint_id> \
  -n <命名空间> \
  [--kubeconfig <kubeconfig路径>] \
  [--pause-time <metav1.Time格式>] \
  [--shutdown-time <metav1.Time格式>]
```

| 参数 | 位置 | 必传 | 默认值 | 说明 |
|------|------|------|--------|------|
| E2B_DOMAIN | 环境变量 | 是 | - | sandbox-manager 域名 |
| E2B_API_KEY | 环境变量 | 是 | - | E2B API Key |
| `--checkpoint` | 命令行 | 是 | - | Checkpoint ID（`upgrade_sts.py` 输出的 snapshot_id） |
| `-n`, `--namespace` | 命令行 | 是 | - | Checkpoint 所在命名空间 |
| `--kubeconfig` | 命令行 | 否 | 默认kubeconfig | kubeconfig文件路径，用于通过 Kubernetes Python 客户端查询 Checkpoint CR |
| `--pause-time` | 命令行 | 否 | - | 绝对暂停时间，metav1.Time 格式（如 `2026-07-23T16:07:44Z`）。设置后沙箱将在该时间自动暂停 |
| `--shutdown-time` | 命令行 | 否 | - | 绝对关闭时间，metav1.Time 格式（如 `2026-07-23T16:07:44Z`）。设置后沙箱将在该时间自动关闭 |

> `--pause-time` 和 `--shutdown-time` 互斥，不能同时指定。若都不指定，则设为 never-timeout。

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

1. **Step 1**: 通过 Kubernetes API 查询 Checkpoint CR，根据 `status.checkpointId` 匹配，获取 `spec.podName` 作为沙箱名称
2. **Step 2**: 通过 E2B SDK `get_info()` 检查沙箱是否已存在
   - 若沙箱处于 Running/Paused 状态：直接退出（无需恢复）
   - 若沙箱处于 dead 等不可读状态：报错退出（需手动清理）
   - 若沙箱不存在（SandboxNotFoundException）：继续恢复
3. **Step 3**: 从 checkpoint 克隆新沙箱，根据 `--pause-time`/`--shutdown-time` 设置超时策略

### 4.6 失败处理

| 失败步骤 | 可能原因 | 处理方式 |
|----------|----------|----------|
| [1] Lookup Checkpoint | Checkpoint CR 不存在或 Kubernetes API 调用失败 | 确认 checkpoint ID 和命名空间；检查 kubeconfig 路径 |
| [2] Check Sandbox | 沙箱处于 dead 状态 | 手动 `kubectl delete sbx <name> -n <ns>` 后重试 |
| [3] Clone | 409（旧 CR 未清理完）或 504（ALB 超时） | 脚本已内置重试；若仍失败，等待 1 分钟后重跑 |

---

## 五、注意事项

1. **sandbox 升级后标签会变**：快照重建后 `sandbox-template` 标签会带 hash 后缀，`sandbox-pool` 标签可能丢失。

2. **Python 依赖**：执行脚本的 Python 解释器必须已安装 `e2b`、`e2b_code_interpreter` 和 `kubernetes` 包，建议使用虚拟环境路径。`kubernetes` 包可通过 `pip install kubernetes` 安装。

3. **超时策略保留**：不指定 `--timeout` 时，脚本自动检测原沙箱的 ShutdownTime。若原沙箱无 ShutdownTime（never-timeout），新沙箱也设为 never-timeout；否则使用默认超时。指定 `--timeout` 则覆盖原策略。

4. **休眠状态感知**：脚本在连接沙箱前（Step 2）通过 E2B SDK `get_info()` 检测原沙箱是否处于休眠状态。若原沙箱休眠中：
   - `--pause-after 0`（默认）：升级完成后立即通过 SDK `pause()` 暂停新沙箱
   - `--pause-after N`（N > 0）：创建时传入 `lifecycle={'on_timeout': 'pause'}` 和 `timeout=N`，由服务端在 N 秒后自动暂停
   - 若原沙箱非休眠状态，跳过此步骤

5. **CSI 配置自动转换**：脚本优先读取 `csi-volume-config` 注解（多卷挂载格式）。若该注解不存在，脚本会回退检查旧版单卷挂载注解（`csi-volume-name`、`csi-mount-point`、`csi-subpath`），并自动转换为多卷挂载格式（`readOnly` 默认为 `false`，即读写挂载，因为旧版注解无 `readOnly` 字段）。若两种格式均不存在，脚本在 Step 1 直接退出，不会连接沙箱。在 `prepare_fix_sts.py` 中，转换后还会自动清除旧版单卷注解。

6. **失败不回滚**：任何步骤失败后该 sandbox 保持当前状态，不会自动恢复。需根据脚本输出中 `[N]` 步骤号定位失败原因，手动处理后重跑。

7. **中途 Checkpoint 自动清理**：升级流程最后一步（Step 8），脚本通过 E2B SDK `Sandbox.delete_snapshot()` 删除快照产生的 Checkpoint。对于需要重新休眠的沙箱，在休眠成功后才删除 Checkpoint。该操作为 best-effort：若删除失败仅打印警告，不影响升级流程。

8. **DNS 策略检查**：创建快照前（Step 3），脚本检查 Sandbox CR 的 `dnsPolicy`，若非 `ClusterFirst` 则自动通过 Kubernetes API patch 修正。该操作不等待 Pod 生效，直接继续后续步骤。

