import argparse
import copy
import json
import os
import sys
import time
from datetime import datetime, timezone

from kubernetes import client, config

from e2b import SandboxException, SandboxNotFoundException
from e2b_code_interpreter import Sandbox, SandboxState

# Snapshot wait timeout in seconds (server-side sync wait for checkpoint completion)
SNAPSHOT_WAIT_SUCCESS_SECONDS = 120

# Polling config for waiting sandbox deletion
KILL_WAIT_TIMEOUT = 120
KILL_POLL_INTERVAL = 3

# Clone config: server-side timeout and wait-ready timeout (seconds)
# These control how long the server waits for Pod creation and readiness
CLONE_TIMEOUT_SECONDS = 600
WAIT_READY_TIMEOUT_SECONDS = 600

# Retry config for clone (handles ALB 504, 409 conflict)
RETRY_TIMEOUT = 600
RETRY_INTERVAL = 5

# Pause wait config (seconds)
PAUSE_WAIT_TIMEOUT = 300
PAUSE_POLL_INTERVAL = 5

# Kubernetes client constants for Sandbox CRD
SANDBOX_GROUP = "agents.kruise.io"
SANDBOX_VERSION = "v1alpha1"
SANDBOX_PLURAL = "sandboxes"


def load_k8s_client(kubeconfig=""):
    """Load kubeconfig and return a CustomObjectsApi client."""
    config.load_kube_config(config_file=kubeconfig if kubeconfig else None)
    return client.CustomObjectsApi()


def get_sandbox_cr(api, namespace, name):
    """
    Get a Sandbox CR as a dict via the Kubernetes API.
    Raises RuntimeError on failure.
    """
    try:
        return api.get_namespaced_custom_object(
            group=SANDBOX_GROUP, version=SANDBOX_VERSION,
            namespace=namespace, plural=SANDBOX_PLURAL, name=name,
        )
    except client.ApiException as e:
        raise RuntimeError(
            f"Failed to get sandbox CR {namespace}/{name}: {e.reason} (status {e.status})"
        )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_pv_map(pv_map_str):
    """Parse 'old1:new1;old2:new2' into dict {old1: new1, old2: new2}."""
    result = {}
    for pair in pv_map_str.split(";"):
        pair = pair.strip()
        if not pair:
            continue
        old, new = pair.split(":", 1)
        result[old.strip()] = new.strip()
    return result


def parse_default_cred(cred_str):
    """Parse 'rw' or 'rw:ro' into (rw, ro) tuple. If ro omitted, ro = rw."""
    parts = cred_str.split(":", 1)
    rw = parts[0].strip()
    ro = parts[1].strip() if len(parts) > 1 and parts[1].strip() else rw
    return rw, ro


# ---------------------------------------------------------------------------
# Sandbox state helpers
# ---------------------------------------------------------------------------

def check_sandbox_paused(sandbox_id):
    """
    Check if sandbox is paused via E2B SDK get_info().
    Returns True if paused, False otherwise.
    Raises on any error — fail-fast, do not silently return False.
    """
    try:
        info = Sandbox.get_info(
            sandbox_id=sandbox_id,
        )
        return info.state == SandboxState.PAUSED
    except Exception as e:
        raise RuntimeError(f"Failed to check sandbox pause state for {sandbox_id}: {e}")


def pause_sandbox(sbx):
    """
    Pause a sandbox via E2B SDK pause()/beta_pause().
    Handles SDK version differences: newer SDKs expose pause(),
    older ones expose beta_pause().
    Raises on failure — fail-fast.
    """
    try:
        if hasattr(sbx, "pause"):
            sbx.pause()
        else:
            sbx.beta_pause()
    except Exception as e:
        raise RuntimeError(f"SDK pause failed: {e}")


def wait_for_paused(sandbox_id, timeout, interval):
    """
    Poll until sandbox state is PAUSED via E2B SDK get_info().
    Returns True on success, False on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if check_sandbox_paused(sandbox_id):
            return True
        time.sleep(interval)
    return False


def delete_snapshot(snapshot_id):
    """
    Delete the intermediate checkpoint/snapshot via E2B SDK.
    Raises on failure — fail-fast.
    """
    deleted = Sandbox.delete_snapshot(snapshot_id=snapshot_id)
    if deleted:
        print(f"    Checkpoint deleted: {snapshot_id}")
    else:
        print(f"    Checkpoint not found (already deleted): {snapshot_id}")


# ---------------------------------------------------------------------------
# CSI volume config helpers
# ---------------------------------------------------------------------------

def fetch_legacy_single_volume_config(cr):
    """
    Extract legacy single-volume CSI annotations (csi-volume-name, csi-mount-point,
    csi-subpath) from the given Sandbox CR dict and convert to multi-volume config format.
    Returns a list with a single config dict, or None if csi-volume-name is absent.
    Legacy annotations have no readOnly field, so it defaults to False (read-write).
    """
    annotations = cr.get("metadata", {}).get("annotations", {}) or {}
    volume_name = annotations.get("e2b.agents.kruise.io/csi-volume-name", "")
    mount_point = annotations.get("e2b.agents.kruise.io/csi-mount-point", "")
    sub_path = annotations.get("e2b.agents.kruise.io/csi-subpath", "")

    if not volume_name:
        return None

    print(f"    Found legacy single-volume annotations, converting to multi-volume format")
    print(f"    csi-volume-name: {volume_name}, csi-mount-point: {mount_point}, csi-subpath: {sub_path}")

    config = {"pvName": volume_name, "readOnly": False}
    if mount_point:
        config["mountPath"] = mount_point
    if sub_path:
        config["subPath"] = sub_path
    return [config]


def fetch_and_transform_csi_config(cr, pv_map, cred_rw, cred_ro):
    """
    Extract e2b.agents.kruise.io/csi-volume-config annotation from the given Sandbox CR dict,
    remap PV names, inject credentialProviderName, return modified JSON string.
    If csi-volume-config is absent, falls back to legacy single-volume annotations
    (csi-volume-name, csi-mount-point, csi-subpath) and converts them to multi-volume
    format. Returns None if both are absent.
    """
    annotations = cr.get("metadata", {}).get("annotations", {}) or {}
    raw = annotations.get("e2b.agents.kruise.io/csi-volume-config", "")

    if not raw:
        print("    No csi-volume-config annotation found on sandbox CR")
        legacy_configs = fetch_legacy_single_volume_config(cr)
        if legacy_configs is None:
            print("    No legacy single-volume annotations found either")
            return None
        configs = legacy_configs
    else:
        try:
            configs = json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"    Failed to parse csi-volume-config annotation as JSON: {e}")
            print(f"    Raw value: {raw}")
            raise
    for cfg in configs:
        old_pv = cfg.get("pvName", "")
        if old_pv in pv_map:
            cfg["pvName"] = pv_map[old_pv]
        # Set credentialProviderName based on readOnly
        if "attributes" not in cfg or cfg["attributes"] is None:
            cfg["attributes"] = {}
        if cfg.get("readOnly", False):
            cfg["attributes"]["credentialProviderName"] = cred_ro
        else:
            cfg["attributes"]["credentialProviderName"] = cred_rw

    return json.dumps(configs)


def reverse_csi_config(cr, pv_map, core_v1=None):
    """
    Reverse the STS transformation on csi-volume-config annotation:
    remove credentialProviderName from attributes and map PV names
    back from new to old. Returns the reversed JSON string, or None
    if no csi-volume-config annotation exists.

    When multiple old PVs map to the same new PV in pv_map, queries the
    K8s API (core_v1) for each candidate old PV's creationTimestamp and
    selects the oldest. If core_v1 is None and ambiguity exists, logs a
    warning and picks the first candidate.
    """
    annotations = cr.get("metadata", {}).get("annotations", {}) or {}
    raw = annotations.get("e2b.agents.kruise.io/csi-volume-config", "")
    if not raw:
        return None

    try:
        configs = json.loads(raw)
    except json.JSONDecodeError:
        return None

    # Build reverse pv_map: {new_pv: [old_pv1, old_pv2, ...]}
    reverse_groups = {}
    for old_pv, new_pv in pv_map.items():
        reverse_groups.setdefault(new_pv, []).append(old_pv)

    # Resolve to {new_pv: oldest_old_pv}
    reverse_map = {}
    for new_pv, old_candidates in reverse_groups.items():
        if len(old_candidates) == 1:
            reverse_map[new_pv] = old_candidates[0]
        elif core_v1 is not None:
            oldest_pv = None
            oldest_ts = None
            for cand in old_candidates:
                try:
                    pv = core_v1.read_persistent_volume(name=cand)
                    ts = pv.metadata.creation_timestamp
                    if oldest_ts is None or ts < oldest_ts:
                        oldest_ts = ts
                        oldest_pv = cand
                except client.ApiException:
                    pass
            if oldest_pv is not None:
                reverse_map[new_pv] = oldest_pv
            else:
                print(f"    Warning: could not read creationTimestamp for any candidate of {new_pv}, using first: {old_candidates[0]}")
                reverse_map[new_pv] = old_candidates[0]
        else:
            print(f"    Warning: multiple old PVs for {new_pv} but no core_v1 client, using first: {old_candidates[0]}")
            reverse_map[new_pv] = old_candidates[0]

    for cfg in configs:
        new_pv = cfg.get("pvName", "")
        if new_pv in reverse_map:
            cfg["pvName"] = reverse_map[new_pv]
        attrs = cfg.get("attributes")
        if attrs and "credentialProviderName" in attrs:
            del attrs["credentialProviderName"]
            if not attrs:
                del cfg["attributes"]

    return json.dumps(configs)


def handle_partial_upgrade(api, cr, namespace, name, pv_map):
    """
    Handle a partially-upgraded sandbox: reverse the STS transformation on
    csi-volume-config, remove STS annotations, and update cr in memory so
    the normal upgrade flow can proceed.
    """
    core_v1 = client.CoreV1Api()
    reversed_json = reverse_csi_config(cr, pv_map, core_v1)

    # Annotations to remove (set to None for JSON merge patch delete)
    remove_annotations = {
        "security.agents.kruise.io/storage-auth": None,
        "security.agents.kruise.io/agent-name": None,
        "security.agents.kruise.io/token-status": None,
    }

    patch_annotations = dict(remove_annotations)
    if reversed_json is not None:
        patch_annotations["e2b.agents.kruise.io/csi-volume-config"] = reversed_json

    patch = {"metadata": {"annotations": patch_annotations}}
    try:
        api.patch_namespaced_custom_object(
            group=SANDBOX_GROUP, version=SANDBOX_VERSION,
            namespace=namespace, plural=SANDBOX_PLURAL, name=name, body=patch,
        )
    except client.ApiException as e:
        raise RuntimeError(f"Failed to patch sandbox CR: {e.reason} (status {e.status})")

    # Update cr in memory so Step 1 reads the reversed config
    if reversed_json is not None:
        cr["metadata"]["annotations"]["e2b.agents.kruise.io/csi-volume-config"] = reversed_json
    for key in remove_annotations:
        cr["metadata"]["annotations"].pop(key, None)

    print(f"    CSI config reversed, STS annotations removed")


def fetch_sandbox_timeout_config(cr):
    """
    Extract spec.shutdownTime and spec.pauseTime from the given Sandbox CR dict.
    Returns (shutdown_time, pause_time) as RFC3339 strings, or None for each if not set.
    """
    spec = cr.get("spec", {})
    shutdown_time = spec.get("shutdownTime") or None
    pause_time = spec.get("pauseTime") or None
    return shutdown_time, pause_time


def check_and_fix_spec(api, cr, namespace, name):
    """
    Check and fix sandbox CR: ensure dnsPolicy is ClusterFirst, ensure spec.runtimes
    includes csi and agent-runtime (preserving other runtimes like traffic-proxy),
    and remove legacy initContainers (init, csi-sidecar, csi-agent-sidecar) and
    volumes that are now injected via the runtime mechanism, and remove legacy
    postStart hooks (envd-run.sh) from non-init containers. All fixes are applied
    in a single patch.
    No wait — proceed after patching.
    """
    spec = cr.get("spec", {})
    template_spec = spec.get("template", {}).get("spec", {})

    # --- Check dnsPolicy ---
    current_dns = template_spec.get("dnsPolicy", "")
    need_dns_fix = current_dns != "ClusterFirst"
    if need_dns_fix:
        print(f"    Current dnsPolicy: '{current_dns}', will patch to ClusterFirst")
    else:
        print(f"    DNS policy is already ClusterFirst")

    # --- Check spec.runtimes: ensure csi and agent-runtime are present ---
    runtimes = spec.get("runtimes", []) or []
    runtime_names = {r.get("name") for r in runtimes}
    need_runtimes_fix = False
    new_runtimes = list(runtimes)  # preserve existing runtimes (e.g. traffic-proxy)
    for required in ("csi", "agent-runtime"):
        if required not in runtime_names:
            new_runtimes.append({"name": required})
            need_runtimes_fix = True
            print(f"    Adding runtime '{required}' to spec.runtimes")
    if not need_runtimes_fix:
        print(f"    spec.runtimes already includes csi and agent-runtime")

    # --- Check initContainers: remove legacy sidecar containers ---
    init_containers = template_spec.get("initContainers", []) or []
    remove_names = {"init", "csi-sidecar", "csi-agent-sidecar"}
    filtered_init = [c for c in init_containers if c.get("name") not in remove_names]
    need_init_fix = len(filtered_init) < len(init_containers)
    if need_init_fix:
        removed = [c.get("name") for c in init_containers if c.get("name") in remove_names]
        print(f"    Removing legacy initContainers: {', '.join(removed)}")
    else:
        print(f"    No legacy sidecar initContainers to remove")

    # --- Check volumes: remove legacy sidecar volumes ---
    volumes = template_spec.get("volumes", []) or []
    remove_vol_names = {
        "envd-volume", "fuse-device", "mount-root", "nas-plugin-dir",
        "oss-plugin-dir", "run-cnfs", "efc-metrics-dir",
        "ossfs-metrics-dir", "csi-agent-config", "token-volume",
    }
    filtered_volumes = [v for v in volumes if v.get("name") not in remove_vol_names]
    need_volumes_fix = len(filtered_volumes) < len(volumes)
    if need_volumes_fix:
        removed_vols = [v.get("name") for v in volumes if v.get("name") in remove_vol_names]
        print(f"    Removing legacy volumes: {', '.join(removed_vols)}")
    else:
        print(f"    No legacy sidecar volumes to remove")

    # --- Check containers: remove legacy postStart hooks (envd-run.sh) ---
    containers = template_spec.get("containers", []) or []
    new_containers = copy.deepcopy(containers)
    need_poststart_fix = False
    for c in new_containers:
        lifecycle = c.get("lifecycle")
        if not lifecycle:
            continue
        post_start = lifecycle.get("postStart")
        if not post_start:
            continue
        command = post_start.get("exec", {}).get("command", [])
        if "/mnt/envd/envd-run.sh" in command:
            del lifecycle["postStart"]
            if not lifecycle:
                del c["lifecycle"]
            need_poststart_fix = True
            print(f"    Removing postStart hook from container '{c.get('name')}'")
    if not need_poststart_fix:
        print(f"    No legacy postStart hooks to remove")

    # --- Check containers: remove legacy sidecar volumeMounts ---
    need_volumemount_fix = False
    for c in new_containers:
        mounts = c.get("volumeMounts", []) or []
        filtered_mounts = [m for m in mounts if m.get("name") not in remove_vol_names]
        if len(filtered_mounts) < len(mounts):
            c["volumeMounts"] = filtered_mounts
            need_volumemount_fix = True
            removed_mounts = [m.get("name") for m in mounts if m.get("name") in remove_vol_names]
            print(f"    Removing volumeMounts from container '{c.get('name')}': {', '.join(removed_mounts)}")
    if not need_volumemount_fix:
        print(f"    No legacy sidecar volumeMounts to remove")

    # --- Check containers: remove legacy runtime env vars ---
    remove_env_names = {"ENVD_DIR", "POD_UID", "GODEBUG"}
    need_env_fix = False
    for c in new_containers:
        envs = c.get("env", []) or []
        filtered_envs = [e for e in envs if e.get("name") not in remove_env_names]
        if len(filtered_envs) < len(envs):
            c["env"] = filtered_envs
            need_env_fix = True
            removed_envs = [e.get("name") for e in envs if e.get("name") in remove_env_names]
            print(f"    Removing env vars from container '{c.get('name')}': {', '.join(removed_envs)}")
    if not need_env_fix:
        print(f"    No legacy runtime env vars to remove")

    # --- Apply patch if any fixes are needed ---
    if not (need_dns_fix or need_runtimes_fix or need_init_fix
            or need_volumes_fix or need_poststart_fix
            or need_volumemount_fix or need_env_fix):
        return

    spec_patch = {}
    if need_dns_fix:
        spec_patch["template"] = {"spec": {"dnsPolicy": "ClusterFirst"}}
        spec_patch["upgradePolicy"] = None
    if need_runtimes_fix:
        spec_patch["runtimes"] = new_runtimes
    if need_init_fix:
        if "template" not in spec_patch:
            spec_patch["template"] = {"spec": {}}
        spec_patch["template"]["spec"]["initContainers"] = filtered_init
    if need_volumes_fix:
        if "template" not in spec_patch:
            spec_patch["template"] = {"spec": {}}
        spec_patch["template"]["spec"]["volumes"] = filtered_volumes
    if need_poststart_fix or need_volumemount_fix or need_env_fix:
        if "template" not in spec_patch:
            spec_patch["template"] = {"spec": {}}
        spec_patch["template"]["spec"]["containers"] = new_containers

    patch = {"spec": spec_patch}
    try:
        api.patch_namespaced_custom_object(
            group=SANDBOX_GROUP, version=SANDBOX_VERSION,
            namespace=namespace, plural=SANDBOX_PLURAL, name=name,
            body=patch,
        )
    except client.ApiException as e:
        raise RuntimeError(f"Failed to patch sandbox CR: {e.reason} (status {e.status})")

# ---------------------------------------------------------------------------
# Clone helper
# ---------------------------------------------------------------------------

def clone_from_snapshot(snapshot_id, sandbox_id, metadata, timeout=0, auto_pause=False):
    """
    Create a new sandbox from a snapshot, handling 409 (old CR deleting)
    and 504 (ALB timeout) with retries. Returns the new Sandbox instance.
    Domain and API key are read from E2B_DOMAIN/E2B_API_KEY env vars by the SDK.
    When auto_pause is True, the server will auto-pause the sandbox after
    the timeout expires (lifecycle={'on_timeout': 'pause'}).
    """
    create_kwargs = {
        "template": snapshot_id,
        "timeout": timeout,
        "metadata": metadata,
    }
    if auto_pause:
        create_kwargs["lifecycle"] = {"on_timeout": "pause"}

    def _create():
        return Sandbox.create(**create_kwargs)

    try:
        new_sbx = _create()
        print(f"    New sandbox created: {new_sbx.sandbox_id}")
        return new_sbx
    except SandboxException as e:
        err_str = str(e)
        if "409" in err_str:
            print(f"    old CR still deleting, waiting...")
            create_deadline = time.time() + RETRY_TIMEOUT
            while time.time() < create_deadline:
                time.sleep(RETRY_INTERVAL)
                try:
                    new_sbx = _create()
                    print(f"    New sandbox created: {new_sbx.sandbox_id}")
                    return new_sbx
                except SandboxException as retry_err:
                    if "409" in str(retry_err):
                        continue
                    raise
            raise TimeoutError(f"Sandbox {sandbox_id} still deleting after {RETRY_TIMEOUT}s")
        elif "504" not in err_str:
            raise
        print(f"    Got ALB 504, sandbox may still be creating on server. Polling...")
        deadline = time.time() + RETRY_TIMEOUT
        while time.time() < deadline:
            time.sleep(RETRY_INTERVAL)
            try:
                new_sbx = Sandbox.connect(
                    sandbox_id=sandbox_id,
                    timeout=300,
                )
                print(f"    Sandbox is up! id: {new_sbx.sandbox_id}")
                return new_sbx
            except SandboxNotFoundException:
                continue
        raise TimeoutError(f"Sandbox {sandbox_id} not ready after {RETRY_TIMEOUT}s")


# ---------------------------------------------------------------------------
# Main upgrade flow
# ---------------------------------------------------------------------------

class UpgradeSkipped(Exception):
    """Raised when a sandbox is skipped (already upgraded, not E2B-owned, etc.)."""
    pass


def main(args):
    """
    Execute the sandbox upgrade flow (snapshot → kill → clone → re-pause).

    Can be called directly from another Python script. The caller must ensure
    E2B_DOMAIN and E2B_API_KEY environment variables are set before calling.

    Args:
        args: An object with the following attributes:
            - name (str): Sandbox name (metadata.name)
            - namespace (str): Sandbox namespace
            - pv_map (str): PV mapping, e.g. 'old1:new1;old2:new2'
            - default_cred (str): CredentialProvider, e.g. 'rw' or 'rw:ro'
            - agent_name (str): Agent application name
            - kubeconfig (str): Path to kubeconfig ("" for default)
            - timeout (int or None): Sandbox timeout in seconds
            - gc_checkpoint (bool): Delete intermediate checkpoint after upgrade
    """
    SANDBOX_NAME = args.name
    SANDBOX_NAMESPACE = args.namespace
    sandbox_id = f"{SANDBOX_NAMESPACE}--{SANDBOX_NAME}"

    # Initialize Kubernetes API client
    api = load_k8s_client(args.kubeconfig)

    # Pre-check: verify sandbox was created via E2B (has agents.kruise.io/owner annotation)
    cr = get_sandbox_cr(api, SANDBOX_NAMESPACE, SANDBOX_NAME)
    annotations = cr.get("metadata", {}).get("annotations", {}) or {}
    if not annotations.get("agents.kruise.io/owner"):
        raise UpgradeSkipped(
            f"Sandbox {SANDBOX_NAMESPACE}/{SANDBOX_NAME} has no 'agents.kruise.io/owner' annotation, "
            f"not created via E2B, skipping upgrade")
    if annotations.get("security.agents.kruise.io/agent-name"):
        has_sts = bool(annotations.get("security.agents.kruise.io/storage-auth"))
        runtimes = cr.get("spec", {}).get("runtimes", []) or []
        runtime_names = {r.get("name") for r in runtimes}
        has_runtimes = "csi" in runtime_names and "agent-runtime" in runtime_names

        if has_sts and not has_runtimes:
            print(f"    Partially upgraded: has STS annotations but no runtimes injection, rolling back...")
            pv_map = parse_pv_map(args.pv_map)
            handle_partial_upgrade(api, cr, SANDBOX_NAMESPACE, SANDBOX_NAME, pv_map)
            # Do NOT skip -- continue with normal upgrade flow
        else:
            raise UpgradeSkipped(
                f"Sandbox {SANDBOX_NAMESPACE}/{SANDBOX_NAME} already upgraded, skipping")

    # ── Step 1: Read and transform CSI volume config ──

    print(f"[1] Reading CSI volume config from sandbox CR...")

    pv_map = parse_pv_map(args.pv_map)
    cred_rw, cred_ro = parse_default_cred(args.default_cred)
    csi_config_json = fetch_and_transform_csi_config(
        cr, pv_map, cred_rw, cred_ro
    )
    if csi_config_json:
        print(f"    Transformed CSI config: {csi_config_json}")
    else:
        raise UpgradeSkipped(f"No CSI config found on sandbox CR, skip STS upgrade")

    shutdown_time, pause_time = fetch_sandbox_timeout_config(cr)
    if shutdown_time:
        print(f"    Original sandbox ShutdownTime: {shutdown_time}")
    else:
        print(f"    Original sandbox has no ShutdownTime (never-timeout)")
    if pause_time:
        print(f"    Original sandbox PauseTime: {pause_time}")

    # ── Step 2: Connect to the existing sandbox ──

    print(f"[2] Connecting to sandbox: {sandbox_id}")

    # Check if sandbox is paused before connecting (connect will auto-resume)
    was_paused = check_sandbox_paused(sandbox_id)
    if was_paused:
        print(f"    Original sandbox is paused, will re-pause after upgrade")

    try:
        sbx = Sandbox.connect(
            sandbox_id=sandbox_id,
        )
    except Exception as e:
        print(f"[2] Failed to connect: {e}")
        raise
    print(f"    Connected. sandbox id: {sbx.sandbox_id}")

    # ── Step 3: Ensure DNS policy is ClusterFirst ──

    print(f"[3] Checking DNS policy...")
    check_and_fix_spec(api, cr, SANDBOX_NAMESPACE, SANDBOX_NAME)

    # ── Step 4: Create a snapshot ──

    # Wait for DNS policy patch to take effect before snapshotting
    print(f"    Waiting 5s for DNS policy to take effect...")
    time.sleep(5)

    print(f"[4] Creating snapshot...")
    try:
        snapshot_info = sbx.create_snapshot(
            headers={
                "x-e2b-kruise-snapshot-keep-running": "false",
                "x-e2b-kruise-snapshot-wait-success-seconds": str(SNAPSHOT_WAIT_SUCCESS_SECONDS),
            },
        )
    except Exception as e:
        print(f"[4] Failed to create snapshot: {e}")
        raise
    snapshot_id = snapshot_info.snapshot_id
    print(f"    Snapshot created: {snapshot_id}")

    # Print restore command for recovery if subsequent steps fail
    restore_cmd = f"python3 restore_from_cp.py --checkpoint {snapshot_id} -n {SANDBOX_NAMESPACE}"
    if pause_time:
        restore_cmd += f" --pause-time {pause_time}"
    elif shutdown_time:
        restore_cmd += f" --shutdown-time {shutdown_time}"
    print(f"    If upgrade fails after this point, restore with:")
    print(f"    {restore_cmd}")

    # ── Step 5: Kill the original sandbox and wait for it to be fully gone ──

    print(f"[5] Killing original sandbox: {sandbox_id}")
    sbx.kill()
    print(f"    Kill request sent, waiting for sandbox to be fully removed...")

    deadline = time.time() + KILL_WAIT_TIMEOUT
    while time.time() < deadline:
        try:
            Sandbox.get_info(
                sandbox_id=sandbox_id,
            )
            time.sleep(KILL_POLL_INTERVAL)
        except SandboxNotFoundException:
            break
        except Exception:
            # Sandbox may be in 'dead' state which SDK can't parse; keep polling
            time.sleep(KILL_POLL_INTERVAL)
    else:
        raise TimeoutError(f"Sandbox {sandbox_id} still exists after {KILL_WAIT_TIMEOUT}s")

    print(f"    Sandbox fully removed.")

    # ── Step 6: Recreate sandbox from snapshot with the same name ──

    print(f"[6] Creating new sandbox from snapshot '{snapshot_id}' with name '{SANDBOX_NAME}'...")

    # Determine timeout policy:
    # 1. pause_time set: auto_pause with remaining from pause_time (preserve original config)
    # 2. --timeout specified: use that timeout
    # 3. No shutdownTime: never-timeout
    # 4. Else: remaining from shutdownTime
    use_auto_pause = False

    if pause_time:
        use_auto_pause = True
        never_timeout = False
        if args.timeout is not None:
            clone_timeout = args.timeout
            print(f"    Using pause timeout: {clone_timeout}s (pauseTime: {pause_time})")
        else:
            pause_dt = datetime.fromisoformat(pause_time.replace("Z", "+00:00"))
            now_dt = datetime.now(timezone.utc)
            clone_timeout = max(600, int((pause_dt - now_dt).total_seconds()))
            print(f"    Using auto-pause with remaining timeout: {clone_timeout}s (pauseTime: {pause_time})")
    elif shutdown_time is None:
        clone_timeout = 0
        never_timeout = True
    elif args.timeout is not None:
        clone_timeout = args.timeout
        never_timeout = False
    else:
        # No --timeout specified, sandbox was running, has shutdownTime.
        # Use remaining time from original sandbox's shutdownTime.
        shutdown_dt = datetime.fromisoformat(shutdown_time.replace("Z", "+00:00"))
        now_dt = datetime.now(timezone.utc)
        clone_timeout = max(600, int((shutdown_dt - now_dt).total_seconds()))
        never_timeout = False
        if clone_timeout > 0:
            print(f"    Using remaining timeout from original sandbox: {clone_timeout}s (shutdownTime: {shutdown_time})")
        else:
            print(f"    Original shutdownTime has passed, using default timeout")

    metadata = {
        "e2b.agents.kruise.io/sandbox-name": SANDBOX_NAME,
        "e2b.agents.kruise.io/reserve-failed-sandbox-for": "forever",
        "e2b.agents.kruise.io/csi-volume-config": csi_config_json,
        "security.agents.kruise.io/agent-name": args.agent_name
    }
    if never_timeout:
        metadata["e2b.agents.kruise.io/never-timeout"] = "true"

    new_sbx = clone_from_snapshot(snapshot_id, sandbox_id, metadata,
                                  timeout=clone_timeout, auto_pause=use_auto_pause)
    print(f"    Done!")

    # ── Step 7: Re-pause if original sandbox was paused ──

    if use_auto_pause:
        print(f"[7] Auto-pause enabled, sandbox will be auto-paused by server.")
        print(f"    No need to wait, upgrade is complete.")
    elif was_paused:
        print(f"[7] Re-pausing sandbox (original was paused)...")
        try:
            pause_sandbox(new_sbx)
        except Exception:
            print(f"    Please manually pause it")
            raise
        print(f"    Pause request sent, waiting for sandbox to be paused...")
        if wait_for_paused(sandbox_id, PAUSE_WAIT_TIMEOUT, PAUSE_POLL_INTERVAL):
            print(f"    Sandbox paused.")
        else:
            raise TimeoutError(f"Timeout waiting for sandbox to pause after {PAUSE_WAIT_TIMEOUT}s")
    else:
        print(f"[7] Skipping re-pause (original was not paused)")

    # ── Step 8: Delete intermediate checkpoint ──

    if args.gc_checkpoint:
        print(f"[8] Deleting intermediate checkpoint: {snapshot_id}")
        delete_snapshot(snapshot_id)
    else:
        print(f"[8] Skipping checkpoint deletion (--gc-checkpoint not set): {snapshot_id}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # E2B domain and API key: must be provided via environment variables
    E2B_DOMAIN = os.environ.get("E2B_DOMAIN")
    E2B_API_KEY = os.environ.get("E2B_API_KEY")
    if not E2B_DOMAIN or not E2B_API_KEY:
        print("Error: E2B_DOMAIN and E2B_API_KEY environment variables are required")
        sys.exit(1)

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Upgrade a single sandbox via snapshot rebuild",
        formatter_class=argparse.RawTextHelpFormatter,
        usage="%(prog)s [-h]\n"
              "       --name NAME\n"
              "       [-n NAMESPACE]\n"
              "       --pv-map PV_MAP\n"
              "       --default-cred DEFAULT_CRED\n"
              "       --agent-name AGENT_NAME\n"
              "       [--kubeconfig KUBECONFIG]\n"
              "       [--timeout TIMEOUT]\n"
              "       [--gc-checkpoint]")
    parser.add_argument("--name", required=True, help="Sandbox name (metadata.name)")
    parser.add_argument("-n", "--namespace", default="default", help="Sandbox namespace (default: default)")
    parser.add_argument("--pv-map", required=True,
        help="PV name mapping: old-pv:new-pv pairs separated by ';'\n"
             "e.g. 'oss-aksk-pv-1:oss-sts-pv-1;oss-aksk-pv-2:oss-sts-pv-2'")
    parser.add_argument("--default-cred", required=True,
        help="Default CredentialProvider names:\n"
             "  'rw-provider' or 'rw-provider:ro-provider'\n"
             "If ro-provider is omitted, rw-provider is used for both\n"
             "read-write and read-only mounts.")
    parser.add_argument("--agent-name", required=True,
        help="Agent application name for the sandbox\n"
             "(value for security.agents.kruise.io/agent-name annotation)")
    parser.add_argument("--kubeconfig", default="",
        help="Path to kubeconfig file\n"
             "(default: use default kubeconfig)")
    parser.add_argument("--timeout", type=int, default=None,
        help="Sandbox timeout in seconds\n"
             "(default: preserve original sandbox timeout policy)")
    parser.add_argument("--gc-checkpoint", action="store_true", default=False,
        help="Delete intermediate checkpoint after upgrade\n"
             "(default: False, checkpoint is preserved for manual restore)")
    cli_args = parser.parse_args()
    try:
        main(cli_args)
    except UpgradeSkipped as e:
        print(e)
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
