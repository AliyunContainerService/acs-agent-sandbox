"""
Batch upgrade sandboxes to STS via upgrade_sts.main().

Reads a list of sandbox names (one per line) from a file and runs
upgrade_sts.main() for each one in-process, with bounded concurrency.
All sandboxes must live in the namespace given by the 'namespace' key
of the config file.

Outputs (paths are derived from the sandbox list file, not configurable):
  - a status file (<list file>.status): one line per sandbox with its
    brief result (SUCCESS / SKIPPED / FAILED / ABORTED / INTERRUPTED)
  - a detailed log file (<list file>.log): each sandbox's full output
    (stdout plus stderr) written as one contiguous block (never
    interleaved across sandboxes), with [start]/[end] timestamps of
    the upgrade operation; worker stderr is never printed to the console

Fail-fast: -m/--max-failure aborts the batch (no new upgrades) once the
FAILED count exceeds the threshold; in-flight upgrades finish normally.

SIGTERM/SIGINT: stops dispatching new upgrades and interrupts in-flight
workers immediately (recorded as INTERRUPTED).

Config file (-c, default: sts.conf): one 'key=value' per line with the
recognized keys 'namespace', 'pv-map', 'default-cred', 'agent-name',
'kubeconfig', 'timeout' and 'gc-checkpoint'; their values are passed to
upgrade_sts.main() as -n/--pv-map/--default-cred/--agent-name/
--kubeconfig/--timeout/--gc-checkpoint respectively. All upgrade settings
come from this file; there are no CLI equivalents.

Before any upgrade starts, the config is pre-validated against the cluster:
pv-map target PVs must exist with spec volumeAttributes authType
'agent-identity'; default-cred CredentialProviders must exist with
spec.type 'RAM'; the agent-name AgentIdentity must exist.
"""

import argparse
import concurrent.futures
import ctypes
import io
import os
import signal
import sys
import threading
import time
from datetime import datetime

from kubernetes import client, config

import upgrade_sts

# Grace period (seconds) for workers to unwind after SIGTERM injection
SIGTERM_GRACE_SECONDS = 10

# Pre-validation expectations for the upgrade config
CREDENTIAL_PROVIDER_PLURAL = "credentialproviders"
AGENT_IDENTITY_PLURAL = "agentidentities"
EXPECTED_PV_AUTH_TYPE = "agent-identity"
EXPECTED_CRED_TYPE = "RAM"

# Known API group serving CredentialProvider and AgentIdentity; discovery
# prefers this group and falls back to any other group serving the plural
AGENT_IDENTITY_API_GROUP = "agentidentity.alibabacloud.com"

# API discovery cache: plural -> (group, version, namespaced)
_RESOURCE_CACHE = {}


class UpgradeInterrupted(Exception):
    """Raised inside a worker thread when the batch receives SIGTERM."""
    pass


class ThreadOutputRouter(io.TextIOBase):
    """Routes writes to a per-thread buffer; falls back to the real stdout."""

    def __init__(self, fallback):
        self.fallback = fallback
        self.buffers = {}  # thread ident -> io.StringIO

    def bind(self):
        self.buffers[threading.get_ident()] = io.StringIO()

    def unbind(self):
        return self.buffers.pop(threading.get_ident(), None)

    def get_buffer(self, ident):
        return self.buffers.get(ident)

    def write(self, s):
        buf = self.buffers.get(threading.get_ident())
        return (buf or self.fallback).write(s)

    def flush(self):
        buf = self.buffers.get(threading.get_ident())
        (buf or self.fallback).flush()


class BatchState:
    """Shared batch state: counters, abort flag, files, and worker registry."""

    def __init__(self, status_path, log_path, max_failure):
        self.lock = threading.Lock()
        self.abort = threading.Event()          # set by -m threshold or SIGTERM
        self.sigterm = threading.Event()        # set only by SIGTERM/SIGINT
        self.max_failure = max_failure
        self.counts = {"SUCCESS": 0, "SKIPPED": 0, "FAILED": 0,
                       "ABORTED": 0, "INTERRUPTED": 0}
        self.running = {}                       # thread ident -> (id, start ts)
        self.recorded = set()                   # sandbox ids already written
        self.status_file = open(status_path, "a")
        self.log_file = open(log_path, "a")

    def record(self, sandbox_id, status, detail="", output=None,
               started_at=None, finished_at=None):
        """Append one status line and one contiguous log block. Thread-safe."""
        with self.lock:
            if sandbox_id in self.recorded:
                return
            self.recorded.add(sandbox_id)
            self.counts[status] += 1

            line = f"{sandbox_id}  {status}"
            if detail:
                line += f"  {detail}"
            self.status_file.write(line + "\n")
            self.status_file.flush()

            self.log_file.write(f"=== {sandbox_id} ===\n")
            if started_at:
                self.log_file.write(f"[start] {format_ts(started_at)}\n")
            if output:
                self.log_file.write(output)
                if not output.endswith("\n"):
                    self.log_file.write("\n")
            if detail:
                self.log_file.write(f"{detail}\n")
            if finished_at:
                end_line = f"[end] {format_ts(finished_at)}"
                if started_at:
                    elapsed = (finished_at - started_at).total_seconds()
                    end_line += f" (elapsed {int(elapsed)}s)"
                self.log_file.write(end_line + "\n")
            self.log_file.write(f"=== END {sandbox_id} ({status}) ===\n\n")
            self.log_file.flush()

    def failed_over_threshold(self):
        with self.lock:
            return (self.max_failure is not None
                    and self.counts["FAILED"] > self.max_failure)

    def close(self):
        self.status_file.close()
        self.log_file.close()


def format_ts(ts):
    """Format a datetime for the detailed log file."""
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def combine_output(out_value, err_value):
    """Combine a worker's buffered stdout and stderr into one log block."""
    output = out_value or ""
    err = err_value or ""
    if err:
        if output and not output.endswith("\n"):
            output += "\n"
        output += "--- stderr ---\n" + err
    return output or None


def parse_sandbox_list(path):
    """
    Parse the sandbox list file: one sandbox name per line.
    Blank lines and '#' comments are skipped. Returns (names, bad_lines)
    where names is a list of sandbox names and bad_lines is a list
    of raw malformed lines.
    """
    names = []
    bad_lines = []
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "/" in line:
                bad_lines.append(line)
                continue
            names.append(line)
    return names, bad_lines


# Keys recognized in the config file (-c) and the matching CLI attributes
CONFIG_KEY_TO_ATTR = {
    "namespace": "namespace",
    "pv-map": "pv_map",
    "default-cred": "default_cred",
    "agent-name": "agent_name",
    "kubeconfig": "kubeconfig",
    "timeout": "timeout",
    "gc-checkpoint": "gc_checkpoint",
}


def parse_config_file(path):
    """
    Parse the config file: one 'key=value' per line; blank lines and '#'
    comments are skipped. Recognized keys: namespace, pv-map, default-cred,
    agent-name, kubeconfig, timeout, gc-checkpoint. Values are converted to
    the type of the matching upgrade_sts argument (timeout -> int,
    gc-checkpoint -> bool). Exits with an error on malformed lines, unknown
    keys or invalid values.
    """
    cfg = {}
    with open(path) as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                print(f"Error: {path}:{lineno}: malformed line, expected 'key=value'")
                sys.exit(1)
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key not in CONFIG_KEY_TO_ATTR:
                print(f"Error: {path}:{lineno}: unknown key '{key}' "
                      f"(recognized keys: {', '.join(CONFIG_KEY_TO_ATTR)})")
                sys.exit(1)
            attr = CONFIG_KEY_TO_ATTR[key]
            if attr == "timeout":
                try:
                    value = int(value)
                except ValueError:
                    print(f"Error: {path}:{lineno}: invalid timeout value '{value}', "
                          f"expected an integer")
                    sys.exit(1)
            elif attr == "gc_checkpoint":
                lowered = value.lower()
                if lowered in ("true", "1"):
                    value = True
                elif lowered in ("false", "0"):
                    value = False
                else:
                    print(f"Error: {path}:{lineno}: invalid gc-checkpoint value '{value}', "
                          f"expected true/false")
                    sys.exit(1)
            cfg[key] = value
    return cfg


def find_custom_resource(plural):
    """
    Locate a custom resource's (group, version, namespaced) via API
    discovery, preferring the known agentidentity.alibabacloud.com group.
    Raises RuntimeError if the cluster does not serve the resource.
    """
    cached = _RESOURCE_CACHE.get(plural)
    if cached:
        return cached
    candidates = []
    for group in client.ApisApi().get_api_versions().groups:
        if not group.preferred_version:
            continue
        version = group.preferred_version.version
        try:
            resource_list = client.CustomObjectsApi().get_api_resources(
                group.name, version)
        except (client.ApiException, ValueError):
            # ValueError: the client rejects a resource list missing
            # groupVersion (some aggregated API services); skip the group
            continue
        for res in resource_list.resources or []:
            if res.name == plural:
                candidates.append((group.name, version, res.namespaced))
    if not candidates:
        raise RuntimeError(
            f"custom resource '{plural}' not found via API discovery")
    for cand in candidates:
        if cand[0] == AGENT_IDENTITY_API_GROUP:
            _RESOURCE_CACHE[plural] = cand
            return cand
    _RESOURCE_CACHE[plural] = candidates[0]
    return candidates[0]


def get_custom_resource(custom_api, plural, name, namespace):
    """Get a (cluster- or namespace-scoped) custom resource as a dict."""
    group, version, namespaced = find_custom_resource(plural)
    if namespaced:
        return custom_api.get_namespaced_custom_object(
            group, version, namespace, plural, name)
    return custom_api.get_cluster_custom_object(group, version, plural, name)


def validate_upgrade_config(args):
    """
    Pre-validate pv-map / default-cred / agent-name against the cluster
    before any upgrade starts. Prints one OK line per passed check and
    exits with status 1 listing all problems found.
    """
    print("Pre-validating upgrade config against the cluster...")
    config.load_kube_config(
        config_file=args.kubeconfig if args.kubeconfig else None)
    core_v1 = client.CoreV1Api()
    custom = client.CustomObjectsApi()
    problems = []

    # 1. pv-map: every target PV must exist with authType=agent-identity
    if args.pv_map:
        for old, new in upgrade_sts.parse_pv_map(args.pv_map).items():
            label = f"pv-map target PV '{new}' (mapped from '{old}')"
            try:
                pv = core_v1.read_persistent_volume(new)
            except client.ApiException as e:
                problems.append(f"{label}: not found" if e.status == 404 else
                                f"{label}: read failed: {e.reason} (status {e.status})")
                continue
            spec = core_v1.api_client.sanitize_for_serialization(pv).get("spec", {})
            attrs = (spec.get("csi") or {}).get("volumeAttributes") \
                or spec.get("volumeAttributes") or {}
            auth_type = attrs.get("authType")
            if auth_type != EXPECTED_PV_AUTH_TYPE:
                problems.append(
                    f"{label}: volumeAttributes.authType is '{auth_type}', "
                    f"expected '{EXPECTED_PV_AUTH_TYPE}'")
            else:
                print(f"  OK: PV '{new}' exists with authType={EXPECTED_PV_AUTH_TYPE}")

    # 2. default-cred: CredentialProviders must exist with spec.type=RAM
    if args.default_cred:
        cred_rw, cred_ro = upgrade_sts.parse_default_cred(args.default_cred)
        for cred in dict.fromkeys((cred_rw, cred_ro)):
            label = f"credentialprovider '{cred}'"
            try:
                cp = get_custom_resource(
                    custom, CREDENTIAL_PROVIDER_PLURAL, cred, args.namespace)
            except RuntimeError as e:
                problems.append(f"{label}: {e}")
                continue
            except client.ApiException as e:
                problems.append(f"{label}: not found" if e.status == 404 else
                                f"{label}: read failed: {e.reason} (status {e.status})")
                continue
            cred_type = (cp.get("spec") or {}).get("type")
            if cred_type != EXPECTED_CRED_TYPE:
                problems.append(
                    f"{label}: spec.type is '{cred_type}', expected '{EXPECTED_CRED_TYPE}'")
            else:
                print(f"  OK: CredentialProvider '{cred}' exists with type={EXPECTED_CRED_TYPE}")

    # 3. agent-name: the AgentIdentity must exist
    if args.agent_name:
        label = f"agentidentity '{args.agent_name}'"
        try:
            get_custom_resource(
                custom, AGENT_IDENTITY_PLURAL, args.agent_name, args.namespace)
            print(f"  OK: AgentIdentity '{args.agent_name}' exists")
        except RuntimeError as e:
            problems.append(f"{label}: {e}")
        except client.ApiException as e:
            problems.append(f"{label}: not found" if e.status == 404 else
                            f"{label}: read failed: {e.reason} (status {e.status})")

    if problems:
        print("Error: upgrade config pre-validation failed:")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)


def upgrade_one(state, router, err_router, args, name):
    """Worker: run upgrade_sts.main() for a single sandbox.

    Returns the upgrade duration in seconds, or None if the sandbox was
    never started (batch already aborted).
    """
    namespace = args.namespace
    sandbox_id = f"{namespace}/{name}"

    if state.abort.is_set():
        reason = ("received SIGTERM" if state.sigterm.is_set()
                  else f"batch aborted after {state.counts['FAILED']} failures")
        status = "INTERRUPTED" if state.sigterm.is_set() else "ABORTED"
        state.record(sandbox_id, status, reason)
        return

    ident = threading.get_ident()
    started_at = datetime.now()
    with state.lock:
        state.running[ident] = (sandbox_id, started_at)
    router.bind()
    err_router.bind()

    status = "FAILED"
    detail = ""
    try:
        ns = argparse.Namespace(
            name=name,
            namespace=namespace,
            pv_map=args.pv_map,
            default_cred=args.default_cred,
            agent_name=args.agent_name,
            kubeconfig=args.kubeconfig,
            timeout=args.timeout,
            gc_checkpoint=args.gc_checkpoint,
        )
        upgrade_sts.main(ns)
        status = "SUCCESS"
    except upgrade_sts.UpgradeSkipped as e:
        status = "SKIPPED"
        detail = str(e)
    except UpgradeInterrupted:
        status = "INTERRUPTED"
        detail = "received SIGTERM"
    except Exception as e:
        status = "FAILED"
        detail = str(e)
    finally:
        finished_at = datetime.now()
        buf = router.unbind()
        err_buf = err_router.unbind()
        with state.lock:
            state.running.pop(ident, None)

    output = combine_output(buf.getvalue() if buf else None,
                            err_buf.getvalue() if err_buf else None)
    state.record(sandbox_id, status, detail, output,
                 started_at, finished_at)

    if status == "FAILED" and state.failed_over_threshold():
        if not state.abort.is_set():
            state.abort.set()
            # Batch-level notice: write to the real stderr, bypassing
            # the per-thread router so it always reaches the console
            print(f"!! FAILED count exceeded --max-failure={state.max_failure}, "
                  f"aborting batch (in-flight upgrades will finish)",
                  file=err_router.fallback)

    return int((finished_at - started_at).total_seconds())


def install_signal_handlers(state, router, err_router):
    """Register SIGTERM/SIGINT handlers that interrupt in-flight workers."""

    def handler(signum, frame):
        state.sigterm.set()
        state.abort.set()
        sys.stderr.write(f"\n!! Received signal {signum}, interrupting batch...\n")

        with state.lock:
            targets = dict(state.running)  # ident -> sandbox_id

        # Inject UpgradeInterrupted into each running worker thread
        for ident in targets:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(ident), ctypes.py_object(UpgradeInterrupted))

        # Grace period: let workers unwind and record their own status
        deadline = time.time() + SIGTERM_GRACE_SECONDS
        while time.time() < deadline:
            with state.lock:
                if not state.running:
                    return  # all workers unwound; main flow finishes up
            time.sleep(0.2)

        # Fallback: record INTERRUPTED for workers stuck in blocking C calls
        with state.lock:
            stuck = dict(state.running)
        now = datetime.now()
        for ident, (sandbox_id, started_at) in stuck.items():
            buf = router.get_buffer(ident)
            err_buf = err_router.get_buffer(ident)
            output = combine_output(buf.getvalue() if buf else None,
                                    err_buf.getvalue() if err_buf else None)
            state.record(sandbox_id, "INTERRUPTED", "received SIGTERM", output,
                         started_at, now)
        write_summary(state)
        os._exit(130)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def write_summary(state):
    """Print the final summary to console and append it to the status file."""
    c = state.counts
    total = sum(c.values())
    summary = (f"total={total} success={c['SUCCESS']} skipped={c['SKIPPED']} "
               f"failed={c['FAILED']} aborted={c['ABORTED']} "
               f"interrupted={c['INTERRUPTED']}")
    sys.stderr.write(f"\nBatch finished: {summary}\n")
    with state.lock:
        state.status_file.write(f"# {summary}\n")
        state.status_file.flush()


def main():
    # E2B domain and API key: must be provided via environment variables
    if not os.environ.get("E2B_DOMAIN") or not os.environ.get("E2B_API_KEY"):
        print("Error: E2B_DOMAIN and E2B_API_KEY environment variables are required")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Batch upgrade sandboxes via upgrade_sts.main()",
        formatter_class=argparse.RawTextHelpFormatter,
        usage="%(prog)s [-h]\n"
              "       -f FILE\n"
              "       [-c CONFIG]\n"
              "       [-p PARALLELISM]\n"
              "       [-m MAX_FAILURE]")
    parser.add_argument("-f", "--file", required=True,
        help="Path to sandbox list file: one sandbox name per line\n"
             "(blank lines and '#' comments are skipped); the output\n"
             "files are <FILE>.status and <FILE>.log")
    parser.add_argument("-c", "--config", default=None,
        help="Path to upgrade config file: one 'key=value' per line with\n"
             "keys namespace / pv-map / default-cred / agent-name /\n"
             "kubeconfig / timeout / gc-checkpoint; their values are\n"
             "passed to upgrade_sts.main() for every sandbox\n"
             "(default: sts.conf in the current directory)")
    parser.add_argument("-p", "--parallelism", type=int, default=1,
        help="Max concurrent sandbox upgrades (default: 1)")
    parser.add_argument("-m", "--max-failure", type=int, default=None,
        help="Abort the batch once FAILED count exceeds this threshold\n"
             "(default: no limit; SKIPPED does not count)")
    args = parser.parse_args()

    # Defaults for upgrade settings; the config file provides the values
    args.namespace = None
    args.pv_map = None
    args.default_cred = None
    args.agent_name = None
    args.kubeconfig = ""
    args.timeout = None
    args.gc_checkpoint = False

    # Load the config file (-c); default to sts.conf if -c is omitted
    config_path = args.config if args.config else "sts.conf"
    if os.path.isfile(config_path):
        config = parse_config_file(config_path)
        for key, value in config.items():
            setattr(args, CONFIG_KEY_TO_ATTR[key], value)
    else:
        print(f"Error: config file not found: {config_path}")
        sys.exit(1)

    # Validate required settings from the config file
    if not args.namespace:
        print(f"Error: missing required setting in {config_path}: namespace")
        sys.exit(1)

    # Fail fast on invalid pv-map / default-cred / agent-name before any
    # upgrade starts
    validate_upgrade_config(args)

    names, bad_lines = parse_sandbox_list(args.file)
    if not names and not bad_lines:
        print(f"Error: no sandbox entries found in {args.file}")
        sys.exit(1)

    # Output paths are derived from the sandbox list file name
    status_path = args.file + ".status"
    log_path = args.file + ".log"

    state = BatchState(status_path, log_path, args.max_failure)
    router = ThreadOutputRouter(sys.stdout)
    sys.stdout = router
    # Worker stderr must not reach the console: route it through a
    # per-thread buffer and append it to each sandbox's log block
    err_router = ThreadOutputRouter(sys.stderr)
    sys.stderr = err_router
    install_signal_handlers(state, router, err_router)

    # Record malformed lines as FAILED without aborting the batch
    for line in bad_lines:
        state.record(line, "FAILED", "malformed line, expected one sandbox name per line")

    total = len(names)
    sys.stderr.write(f"Upgrading {total} sandboxes in namespace {args.namespace} "
                     f"(parallelism={args.parallelism}, "
                     f"max_failure={args.max_failure})\n")
    sys.stderr.write(f"Status file: {status_path}\n")
    sys.stderr.write(f"Log file:    {log_path}\n\n")

    done = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=args.parallelism) as executor:
            futures = {
                executor.submit(upgrade_one, state, router, err_router,
                                args, name):
                    f"{args.namespace}/{name}"
                for name in names
            }
            for future in concurrent.futures.as_completed(futures):
                sandbox_id = futures[future]
                done += 1
                elapsed = None
                try:
                    elapsed = future.result()
                except Exception as e:
                    # upgrade_one records its own status; this is a safety net
                    state.record(sandbox_id, "FAILED", f"worker error: {e}")
                suffix = f" (elapsed {elapsed}s)" if elapsed is not None else ""
                sys.stderr.write(f"[{done}/{total}] {sandbox_id} done{suffix}\n")
    finally:
        sys.stdout = router.fallback
        sys.stderr = err_router.fallback
        write_summary(state)
        state.close()

    if state.sigterm.is_set():
        sys.exit(130)
    if state.counts["ABORTED"] > 0:
        sys.exit(2)
    if state.counts["FAILED"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
