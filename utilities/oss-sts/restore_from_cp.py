#!/usr/bin/env python3
"""
Restore a sandbox from a pre-existing checkpoint.

Looks up the Checkpoint CR by checkpoint ID to get the sandbox name,
checks if the sandbox already exists (exits if running/paused/dead),
and clones a new sandbox from the checkpoint.

Required: kubernetes Python package, e2b and e2b_code_interpreter SDK packages.
Env vars: E2B_DOMAIN, E2B_API_KEY.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from kubernetes import client, config

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
    description="Restore a sandbox from a pre-existing checkpoint",
    formatter_class=argparse.RawTextHelpFormatter,
    usage="%(prog)s [-h]\n"
          "       --checkpoint CHECKPOINT_ID\n"
          "       -n NAMESPACE\n"
          "       [--kubeconfig KUBECONFIG]\n"
          "       [--pause-time PAUSE_TIME]\n"
          "       [--shutdown-time SHUTDOWN_TIME]")
parser.add_argument("--checkpoint", required=True,
    help="Checkpoint ID (the snapshot_id from upgrade_sts.py output)")
parser.add_argument("-n", "--namespace", required=True,
    help="Namespace to search for the Checkpoint CR")
parser.add_argument("--kubeconfig", default="",
    help="Path to kubeconfig file\n"
         "(default: use default kubeconfig)")
parser.add_argument("--pause-time", default=None,
    help="Absolute pause time in metav1.Time format (e.g. 2026-07-23T16:07:44Z)\n"
         "(sets timeout + auto_pause=True)")
parser.add_argument("--shutdown-time", default=None,
    help="Absolute shutdown time in metav1.Time format (e.g. 2026-07-23T16:07:44Z)\n"
         "(sets timeout, no auto_pause)")
args = parser.parse_args()

# Validate mutual exclusivity
if args.pause_time is not None and args.shutdown_time is not None:
    print("Error: --pause-time and --shutdown-time are mutually exclusive")
    sys.exit(1)

# Retry config for clone (handles ALB 504, 409 conflict)
RETRY_TIMEOUT = 600
RETRY_INTERVAL = 5

# Kubernetes client constants for Sandbox CRD
SANDBOX_GROUP = "agents.kruise.io"
SANDBOX_VERSION = "v1alpha1"
SANDBOX_PLURAL = "sandboxes"

# Kubernetes client constants for Checkpoint CRD
CHECKPOINT_GROUP = "agents.kruise.io"
CHECKPOINT_VERSION = "v1alpha1"
CHECKPOINT_PLURAL = "checkpoints"


# ---------------------------------------------------------------------------
# Kubernetes client helpers
# ---------------------------------------------------------------------------

def load_k8s_client(kubeconfig=""):
    """Load kubeconfig and return a CustomObjectsApi client."""
    config.load_kube_config(config_file=kubeconfig if kubeconfig else None)
    return client.CustomObjectsApi()


def find_checkpoint_cr(api, namespace, checkpoint_id):
    """
    List Checkpoint CRs in the given namespace and find the one matching
    the checkpoint ID (status.checkpointId).
    Returns the Checkpoint CR dict, or None if not found.
    """
    try:
        result = api.list_namespaced_custom_object(
            group=CHECKPOINT_GROUP, version=CHECKPOINT_VERSION,
            namespace=namespace, plural=CHECKPOINT_PLURAL,
        )
    except client.ApiException as e:
        raise RuntimeError(
            f"Failed to list checkpoints in {namespace}: {e.reason} (status {e.status})"
        )

    for cp in result.get("items", []):
        if cp.get("status", {}).get("checkpointId") == checkpoint_id:
            return cp
    return None


# ---------------------------------------------------------------------------
# Clone helper (duplicated from upgrade_sts.py)
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


# ── Step 1: Find Checkpoint CR and get sandbox name ──

print(f"[1] Looking up checkpoint: {args.checkpoint} in namespace {args.namespace}")

api = load_k8s_client(args.kubeconfig)

try:
    checkpoint_cr = find_checkpoint_cr(api, args.namespace, args.checkpoint)
except RuntimeError as e:
    print(f"[1] Error: {e}")
    sys.exit(1)

if checkpoint_cr is None:
    print(f"[1] Error: Checkpoint {args.checkpoint} not found in namespace {args.namespace}")
    sys.exit(1)

sandbox_name = checkpoint_cr.get("spec", {}).get("podName")
if not sandbox_name:
    print(f"[1] Error: Checkpoint {args.checkpoint} has no spec.podName")
    sys.exit(1)

sandbox_id = f"{args.namespace}--{sandbox_name}"
print(f"    Found checkpoint: sandbox_name={sandbox_name}")

# ── Step 2: Check if sandbox already exists ──

print(f"[2] Checking if sandbox already exists: {sandbox_id}")

try:
    info = Sandbox.get_info(sandbox_id=sandbox_id)
    print(f"    Sandbox already exists (state: {info.state}), nothing to do.")
    sys.exit(0)
except SandboxNotFoundException:
    print(f"    Sandbox not found, proceeding to restore.")
except Exception as e:
    print(f"[2] Error: Sandbox exists but in an unreadable state: {e}")
    print(f"    Please manually clean up the sandbox before restoring.")
    sys.exit(1)

# ── Step 3: Clone from checkpoint ──

print(f"[3] Cloning sandbox from checkpoint '{args.checkpoint}' with name '{sandbox_name}'...")

# Determine timeout policy
use_auto_pause = False
never_timeout = False
now_dt = datetime.now(timezone.utc)

if args.pause_time is not None:
    use_auto_pause = True
    pause_dt = datetime.fromisoformat(args.pause_time.replace("Z", "+00:00"))
    clone_timeout = max(0, int((pause_dt - now_dt).total_seconds()))
    print(f"    Using auto-pause: pauseTime={args.pause_time}, timeout={clone_timeout}s")
elif args.shutdown_time is not None:
    shutdown_dt = datetime.fromisoformat(args.shutdown_time.replace("Z", "+00:00"))
    clone_timeout = max(0, int((shutdown_dt - now_dt).total_seconds()))
    print(f"    Using shutdown: shutdownTime={args.shutdown_time}, timeout={clone_timeout}s")
else:
    clone_timeout = 0
    never_timeout = True
    print(f"    Using never-timeout")

# Build minimal metadata
metadata = {
    "e2b.agents.kruise.io/sandbox-name": sandbox_name,
}
if never_timeout:
    metadata["e2b.agents.kruise.io/never-timeout"] = "true"

clone_from_snapshot(args.checkpoint, sandbox_id, metadata,
                     timeout=clone_timeout, auto_pause=use_auto_pause)
print(f"    Done!")
