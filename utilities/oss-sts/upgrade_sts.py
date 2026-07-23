import argparse
import json
import os
import subprocess
import sys
import time

from e2b import SandboxException, SandboxNotFoundException
from e2b_code_interpreter import Sandbox, SandboxState

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
          "       [--pause-after PAUSE_AFTER]")
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
         "(default: use kubectl default config)")
parser.add_argument("--timeout", type=int, default=None,
    help="Sandbox timeout in seconds\n"
         "(default: preserve original sandbox timeout policy)")
parser.add_argument("--pause-after", type=int, default=0,
    help="Timeout in seconds before auto-pausing after upgrade\n"
         "(only applies if original sandbox was paused)\n"
         "(default: 0, pause immediately via SDK)")
args = parser.parse_args()
SANDBOX_NAME = args.name
SANDBOX_NAMESPACE = args.namespace

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
    """
    try:
        info = Sandbox.get_info(
            sandbox_id=sandbox_id,
        )
        return info.state == SandboxState.PAUSED
    except Exception:
        return False


def pause_sandbox(sbx):
    """
    Pause a sandbox via E2B SDK pause()/beta_pause().
    Handles SDK version differences: newer SDKs expose pause(),
    older ones expose beta_pause().
    Returns True on success, False on failure.
    """
    try:
        if hasattr(sbx, "pause"):
            sbx.pause()
        else:
            sbx.beta_pause()
        return True
    except Exception as e:
        print(f"    Warning: SDK pause failed: {e}")
        return False


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


# ---------------------------------------------------------------------------
# CSI volume config helpers
# ---------------------------------------------------------------------------

def fetch_and_transform_csi_config(kubeconfig, namespace, name, pv_map, cred_rw, cred_ro):
    """
    Fetch e2b.agents.kruise.io/csi-volume-config annotation from the Sandbox CR,
    remap PV names, inject credentialProviderName, return modified JSON string.
    Returns None if annotation is absent.
    """
    cmd = ["kubectl"]
    if kubeconfig:
        cmd += ["--kubeconfig", kubeconfig]
    cmd += ["get", "sbx", name, "-n", namespace,
            "-o", "jsonpath={.metadata.annotations['e2b\\.agents\\.kruise\\.io/csi-volume-config']}"]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        print(f"    Warning: kubectl get sbx failed: {result.stderr.strip()}")
        return None

    raw = result.stdout.strip()
    if not raw:
        print("    No csi-volume-config annotation found on sandbox CR")
        return None

    # Normalize smart/curly quotes to straight double quotes for JSON parsing
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


def fetch_sandbox_shutdown_time(kubeconfig, namespace, name):
    """
    Fetch spec.shutdownTime from the Sandbox CR via kubectl.
    Returns the RFC3339 time string if set, or None if not set.
    """
    cmd = ["kubectl"]
    if kubeconfig:
        cmd += ["--kubeconfig", kubeconfig]
    cmd += ["get", "sbx", name, "-n", namespace,
            "-o", "jsonpath={.spec.shutdownTime}"]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        print(f"    Warning: kubectl get sbx for shutdownTime failed: {result.stderr.strip()}")
        return None

    raw = result.stdout.strip()
    if not raw:
        return None
    return raw


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
            print(f"    Got 409 (old CR still deleting), retrying create...")
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
            raise TimeoutError(f"Sandbox {sandbox_id} still conflicting after {RETRY_TIMEOUT}s")
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

# ── Step 1: Connect to the existing sandbox ──

sandbox_id = f"{SANDBOX_NAMESPACE}--{SANDBOX_NAME}"
print(f"[1] Connecting to sandbox: {sandbox_id}")

# Check if sandbox is paused before connecting (connect will auto-resume)
was_paused = check_sandbox_paused(sandbox_id)
if was_paused:
    print(f"    Original sandbox is paused, will re-pause after upgrade")

try:
    sbx = Sandbox.connect(
        sandbox_id=sandbox_id,
        timeout=300,
    )
except Exception as e:
    print(f"[1] Failed to connect: {e}")
    raise
print(f"    Connected. sandbox id: {sbx.sandbox_id}")

# ── Step 2: Read and transform CSI volume config ──

print(f"[2] Reading CSI volume config from sandbox CR...")
pv_map = parse_pv_map(args.pv_map)
cred_rw, cred_ro = parse_default_cred(args.default_cred)
csi_config_json = fetch_and_transform_csi_config(
    args.kubeconfig, SANDBOX_NAMESPACE, SANDBOX_NAME, pv_map, cred_rw, cred_ro
)
if csi_config_json:
    print(f"    Transformed CSI config: {csi_config_json}")
else:
    print(f"    No CSI config found on sandbox CR, cannot proceed with STS upgrade")
    sys.exit(1)

shutdown_time = fetch_sandbox_shutdown_time(args.kubeconfig, SANDBOX_NAMESPACE, SANDBOX_NAME)
if shutdown_time:
    print(f"    Original sandbox ShutdownTime: {shutdown_time}")
else:
    print(f"    Original sandbox has no ShutdownTime (never-timeout)")

# ── Step 3: Create a snapshot ──

print(f"[3] Creating snapshot...")
try:
    snapshot_info = sbx.create_snapshot(
        headers={
            "x-e2b-kruise-snapshot-keep-running": "false",
            "x-e2b-kruise-snapshot-wait-success-seconds": str(SNAPSHOT_WAIT_SUCCESS_SECONDS),
        },
    )
except Exception as e:
    print(f"[3] Failed to create snapshot: {e}")
    raise
snapshot_id = snapshot_info.snapshot_id
print(f"    Snapshot created: {snapshot_id}")

# ── Step 4: Kill the original sandbox and wait for it to be fully gone ──

print(f"[4] Killing original sandbox: {sandbox_id}")
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
        # sandbox may be in 'dead' state which SDK can't parse, treat as gone
        break
else:
    raise TimeoutError(f"Sandbox {sandbox_id} still exists after {KILL_WAIT_TIMEOUT}s")

print(f"    Sandbox fully removed.")

# ── Step 5: Recreate sandbox from snapshot with the same name ──

print(f"[5] Creating new sandbox from snapshot '{snapshot_id}' with name '{SANDBOX_NAME}'...")

# Determine timeout policy: --timeout overrides; otherwise preserve original
# When was_paused and pause_after > 0, use auto_pause with pause_after as timeout
use_auto_pause = was_paused and args.pause_after > 0

if use_auto_pause:
    clone_timeout = args.pause_after
    never_timeout = False
elif args.timeout is not None:
    clone_timeout = args.timeout
    never_timeout = False
elif shutdown_time is None:
    clone_timeout = 0
    never_timeout = True
else:
    clone_timeout = 0
    never_timeout = False

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

# ── Step 6: Re-pause if original sandbox was paused ──

if was_paused:
    if use_auto_pause:
        print(f"[6] Auto-pause enabled (timeout={args.pause_after}s).")
        print(f"    Sandbox will be auto-paused by server after {args.pause_after}s.")
        print(f"    No need to wait, upgrade is complete.")
    else:
        print(f"[6] Re-pausing sandbox (original was paused)...")
        if not pause_sandbox(new_sbx):
            print(f"    Failed to pause sandbox, please manually pause it")
            sys.exit(1)
        print(f"    Pause request sent, waiting for sandbox to be paused...")
        if wait_for_paused(sandbox_id, PAUSE_WAIT_TIMEOUT, PAUSE_POLL_INTERVAL):
            print(f"    Sandbox paused.")
        else:
            print(f"    Timeout waiting for sandbox to pause after {PAUSE_WAIT_TIMEOUT}s")
else:
    print(f"[6] Skipping re-pause (original was not paused)")
