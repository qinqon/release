#!/usr/bin/env python3
"""Wait for the management nodes to settle after CNV has been installed.

CNV's virt-platform-autopilot creates MachineConfigs (psi kernel argument, swap)
that make the MCO reboot every node of the pools selecting them. HyperConverged
Available says nothing about that rollout, so the tests would otherwise start on
a management cluster that is about to lose its nodes.
"""

import json
import os
import re
import subprocess
import sys
import time

TIMEOUT = int(os.environ["MGMT_CONVERGENCE_TIMEOUT"])
QUIET_PERIOD = int(os.environ["MGMT_CONVERGENCE_QUIET_PERIOD"])
AUTOPILOT_MC_WAIT = int(os.environ["MGMT_AUTOPILOT_MC_WAIT"])
AUTOPILOT_MC_PATTERN = re.compile(os.environ["MGMT_AUTOPILOT_MC_PATTERN"])
POLL_INTERVAL = 15
REQUEST_TIMEOUT = 60

HEALTHY = [
    ("machineconfigpool", "Updated=True"),
    ("machineconfigpool", "Updating=False"),
    ("machineconfigpool", "Degraded=False"),
    ("node", "Ready=True"),
]

STARTED = time.monotonic()
DEADLINE = STARTED + TIMEOUT


def log(message):
    print(message, flush=True)


def oc_get(*args):
    return subprocess.run(
        ["oc", "get", *args, f"--request-timeout={REQUEST_TIMEOUT}s"],
        check=True, capture_output=True, text=True, timeout=REQUEST_TIMEOUT + 10,
    ).stdout


def oc_wait(resource, condition, block):
    """Check a condition on every object, blocking until the deadline if asked to."""
    timeout = max(0, int(DEADLINE - time.monotonic())) if block else 0
    result = subprocess.run(
        ["oc", "wait", resource, "--all", f"--for=condition={condition}", f"--timeout={timeout}s"],
        capture_output=True, text=True, timeout=timeout + REQUEST_TIMEOUT, check=False,
    )
    if result.returncode != 0:
        log(f"{resource} {condition} not met: {result.stderr.strip()}")
    return result.returncode == 0


def selects(selector, labels):
    """Evaluate a Kubernetes label selector against a set of labels."""
    if selector is None:
        return False
    for key, value in selector.get("matchLabels", {}).items():
        if labels.get(key) != value:
            return False
    for expression in selector.get("matchExpressions", []):
        key, values = expression["key"], expression.get("values", [])
        matches = {
            "In": labels.get(key) in values and key in labels,
            "NotIn": labels.get(key) not in values,
            "Exists": key in labels,
            "DoesNotExist": key not in labels,
        }[expression["operator"]]
        if not matches:
            return False
    return True


def autopilot_configs():
    """Return whether autopilot MachineConfigs exist, and those not applied yet.

    A pool keeps reporting Updated=True for its previous configuration until the
    MCO renders the new MachineConfigs, so the conditions alone can be stale.
    """
    configs = [
        config["metadata"] for config in json.loads(oc_get("machineconfig", "-o", "json"))["items"]
        if AUTOPILOT_MC_PATTERN.search(config["metadata"]["name"])
    ]
    unapplied = []
    for pool in json.loads(oc_get("machineconfigpool", "-o", "json"))["items"]:
        applied = {source["name"] for source in pool["status"].get("configuration", {}).get("source", [])}
        selector = pool["spec"].get("machineConfigSelector")
        unapplied += [
            f"{pool['metadata']['name']}/{config['name']}" for config in configs
            if selects(selector, config.get("labels", {})) and config["name"] not in applied
        ]
    return bool(configs), unapplied


def converged(block):
    """Return whether autopilot MachineConfigs exist and whether the nodes settled."""
    found, unapplied = autopilot_configs()
    if unapplied:
        log(f"MachineConfigs not applied yet (pool/config): {', '.join(unapplied)}")
        return found, False
    return found, all(oc_wait(resource, condition, block) for resource, condition in HEALTHY)


def has_machine_config_pools():
    try:
        return bool(oc_get("machineconfigpool", "-o", "name").strip())
    except subprocess.SubprocessError as error:
        # Any other failure may be transient, leave it to the retries of the wait.
        return "doesn't have a resource type" not in (getattr(error, "stderr", None) or "")


def diagnostics():
    for args in (
        ["machineconfigpool", "-o", "wide"],
        ["nodes", "-o", "wide"],
        ["machineconfig"],
        ["pods", "-n", "openshift-machine-config-operator", "-o", "wide"],
    ):
        log(f"$ oc get {' '.join(args)}")
        subprocess.run(
            ["oc", "get", *args, f"--request-timeout={REQUEST_TIMEOUT}s"],
            timeout=REQUEST_TIMEOUT + 10, check=False,
        )


def main():
    if not has_machine_config_pools():
        log("The cluster has no MachineConfigPools, nothing to wait for")
        return 0

    log(f"Waiting up to {TIMEOUT}s for the management nodes to settle")
    stable_since = None
    while time.monotonic() < DEADLINE:
        try:
            # Block until the rollout is over, then sample without blocking so that
            # anything going unhealthy during the quiet period restarts it.
            found, ok = converged(block=stable_since is None)
        except (subprocess.SubprocessError, ValueError, KeyError) as error:
            log(f"Could not read the MachineConfig state: {error}")
            found, ok = False, False

        now = time.monotonic()
        if not ok:
            stable_since = None
        elif stable_since is None:
            stable_since = now
            log(f"Management nodes are settled, requiring that to hold for {QUIET_PERIOD}s")
        # Without autopilot MachineConfigs, also give the autopilot time to create them.
        elif now - stable_since >= QUIET_PERIOD and (found or now - STARTED >= AUTOPILOT_MC_WAIT):
            log(f"Management nodes stayed settled for {QUIET_PERIOD}s")
            return 0
        time.sleep(POLL_INTERVAL)

    log(f"Error: the management nodes did not settle within {TIMEOUT}s")
    diagnostics()
    return 1


if __name__ == "__main__":
    sys.exit(main())
