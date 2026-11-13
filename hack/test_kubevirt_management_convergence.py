#!/usr/bin/env python3
"""Offline regression tests for the Python barrier embedded in the install step.

Run with: python3 hack/test_kubevirt_management_convergence.py
"""

import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ci-operator/step-registry/hypershift/kubevirt/install/hypershift-kubevirt-install-commands.sh"
SOURCE = SCRIPT.read_text().split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
BARRIER = types.ModuleType("management_convergence")
exec(compile(SOURCE, str(SCRIPT), "exec"), BARRIER.__dict__)

ROLE = "machineconfiguration.openshift.io/role"
PREFIX = "machineconfiguration.openshift.io/"
CONFIG = {"metadata": {"name": "90-worker-swap-online", "labels": {ROLE: "worker"}}}
POOL = {
    "metadata": {"name": "worker", "generation": 1},
    "spec": {
        "machineConfigSelector": {"matchLabels": {ROLE: "worker"}},
        "configuration": {"name": "rendered-worker-new"},
    },
    "status": {
        "observedGeneration": 1,
        "machineCount": 1, "updatedMachineCount": 1,
        "readyMachineCount": 1, "degradedMachineCount": 0,
        "configuration": {"name": "rendered-worker-new", "source": [{"name": CONFIG["metadata"]["name"]}]},
        "conditions": [
            {"type": "Updated", "status": "True"},
            {"type": "Updating", "status": "False"},
            {"type": "Degraded", "status": "False"},
        ],
    },
}
NODE = {
    "metadata": {"name": "worker-0", "annotations": {
        PREFIX + "currentConfig": "rendered-worker-new",
        PREFIX + "desiredConfig": "rendered-worker-new",
        PREFIX + "state": "Done",
    }},
    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
}


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class ConvergenceTests(unittest.TestCase):
    def setUp(self):
        self.config, self.pool, self.node = copy.deepcopy((CONFIG, POOL, NODE))
        self.clock = Clock()
        self.output = io.StringIO()
        clock_patch = patch.object(BARRIER, "time", self.clock)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)
        redirect = contextlib.redirect_stdout(self.output)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)

    def wait(self, snapshot=None, quiet=30, discovery=60, timeout=120):
        def get_items(_deadline, resource):
            if snapshot:
                snapshot(self.clock.now)
            return {"machineconfig": [self.config], "machineconfigpool": [self.pool], "nodes": [self.node]}[resource]

        with patch.object(BARRIER, "get_items", side_effect=get_items), patch.object(
            BARRIER, "oc", return_value="machineconfigpools.machineconfiguration.openshift.io\n"
        ):
            return BARRIER.wait_for_nodes(timeout, quiet, discovery, "psi-karg|swap-online")

    def test_healthy_cluster_holds_quiet_period(self):
        self.assertTrue(self.wait())
        self.assertEqual(self.clock.now, 30)

    def test_version_without_matching_configs_waits_for_discovery(self):
        self.config["metadata"]["name"] = "unrelated"
        self.assertTrue(self.wait())
        self.assertEqual(self.clock.now, 60)

    def test_no_configs_pays_longer_window_not_sum(self):
        self.config["metadata"]["name"] = "unrelated"
        self.assertTrue(self.wait(quiet=75, discovery=30))
        self.assertEqual(self.clock.now, 75)

    def test_delayed_config_cannot_pass_on_old_pool_status(self):
        def snapshot(now):
            self.config["metadata"]["name"] = "unrelated" if now < 15 else CONFIG["metadata"]["name"]
            self.pool["status"]["configuration"]["source"] = [] if now < 45 else POOL["status"]["configuration"]["source"]

        self.assertTrue(self.wait(snapshot))
        self.assertEqual(self.clock.now, 75)

    def test_known_unapplied_config_never_falls_back(self):
        self.pool["status"]["configuration"]["source"] = []
        self.assertFalse(self.wait(discovery=15, timeout=75))
        self.assertEqual(self.clock.now, 75)

    def test_second_config_resets_quiet_period(self):
        def snapshot(now):
            self.config["metadata"]["name"] = "99-worker-psi-karg" if now >= 15 else CONFIG["metadata"]["name"]
            if now >= 30:
                self.pool["status"]["configuration"]["source"] = [{"name": self.config["metadata"]["name"]}]

        self.assertTrue(self.wait(snapshot))
        self.assertEqual(self.clock.now, 60)

    def test_node_unready_resets_quiet_period(self):
        def snapshot(now):
            self.node["status"]["conditions"][0]["status"] = "False" if now == 15 else "True"

        self.assertTrue(self.wait(snapshot))
        self.assertEqual(self.clock.now, 60)

    def test_api_failure_resets_quiet_period(self):
        def snapshot(now):
            if now == 15:
                raise subprocess.CalledProcessError(1, ["oc", "get"])

        self.assertTrue(self.wait(snapshot))
        self.assertEqual(self.clock.now, 60)

    def test_cannot_succeed_after_slow_read_exhausts_budget(self):
        def snapshot(_now):
            self.clock.now = 120

        self.assertFalse(self.wait(snapshot, quiet=0))

    def test_custom_pool_must_also_apply_worker_config(self):
        custom = copy.deepcopy(self.pool)
        custom["metadata"]["name"] = "virtualization"
        custom["status"]["configuration"]["source"] = []
        pending = BARRIER.pending_pools([self.pool, custom], [self.config])
        self.assertEqual(len(pending), 1)
        self.assertIn("pool virtualization", pending[0])

    def test_config_not_selected_by_pool_is_ignored(self):
        self.config["metadata"]["labels"][ROLE] = "master"
        self.assertEqual(BARRIER.pending_pools([self.pool], [self.config]), [])

    def test_selector_expressions(self):
        cases = [
            ("In", ["worker"], {ROLE: "worker"}, True),
            ("In", ["worker"], {}, False),
            ("NotIn", ["master"], {ROLE: "worker"}, True),
            ("NotIn", ["master"], {}, True),
            ("NotIn", ["worker"], {ROLE: "worker"}, False),
            ("Exists", [], {ROLE: "worker"}, True),
            ("Exists", [], {}, False),
            ("DoesNotExist", [], {}, True),
            ("DoesNotExist", [], {ROLE: "worker"}, False),
        ]
        for operator, values, labels, expected in cases:
            with self.subTest(operator=operator, labels=labels):
                selector = {"matchExpressions": [{"key": ROLE, "operator": operator, "values": values}]}
                self.assertEqual(BARRIER.selector_matches(selector, labels), expected)
        self.assertFalse(BARRIER.selector_matches(None, {}))
        self.assertTrue(BARRIER.selector_matches({}, {}))

    def test_pool_health_checks(self):
        for field, value in (("updatedMachineCount", 0), ("readyMachineCount", 0),
                             ("degradedMachineCount", 1), ("observedGeneration", 0)):
            with self.subTest(field=field):
                pool = copy.deepcopy(self.pool)
                pool["status"][field] = value
                self.assertTrue(BARRIER.pending_pools([pool], [self.config]))
        self.pool["status"]["configuration"]["name"] = "rendered-worker-old"
        self.assertTrue(BARRIER.pending_pools([self.pool], [self.config]))

    def test_pool_conditions(self):
        for condition in ("Updating", "Degraded", "NodeDegraded", "RenderDegraded"):
            with self.subTest(condition=condition):
                pool = copy.deepcopy(self.pool)
                pool["status"]["conditions"].append({"type": condition, "status": "True"})
                self.assertTrue(BARRIER.pending_pools([pool], [self.config]))
        self.pool["status"]["conditions"] = []
        self.assertTrue(BARRIER.pending_pools([self.pool], [self.config]))

    def test_managed_node_config_checks(self):
        for key, value in (("currentConfig", ""), ("desiredConfig", "other"), ("state", "Working")):
            with self.subTest(key=key):
                node = copy.deepcopy(self.node)
                node["metadata"]["annotations"][PREFIX + key] = value
                self.assertTrue(BARRIER.pending_nodes([node]))
        self.node["metadata"]["annotations"] = {}
        self.assertEqual(BARRIER.pending_nodes([self.node]), [])

    def test_absent_api_skips_but_discovery_failure_does_not(self):
        with patch.object(BARRIER, "oc", return_value="machineconfigs.machineconfiguration.openshift.io\n"):
            self.assertTrue(BARRIER.wait_for_nodes(120, 30, 60, "swap-online"))
        with patch.object(BARRIER, "oc", side_effect=subprocess.CalledProcessError(1, ["oc"])) as oc:
            with self.assertRaises(subprocess.CalledProcessError):
                BARRIER.wait_for_nodes(120, 30, 60, "swap-online")
            self.assertEqual(oc.call_count, 3)

    def test_invalid_or_empty_listings_are_errors(self):
        for output in ("", "{}", '{"items": null}', '{"items": []}'):
            with self.subTest(output=output), patch.object(BARRIER, "oc", return_value=output):
                with self.assertRaises((ValueError, KeyError)):
                    BARRIER.get_items(120, "nodes")
        with patch.object(BARRIER, "oc", return_value=json.dumps({"items": []})):
            self.assertEqual(BARRIER.get_items(120, "machineconfig"), [])

    def test_subprocess_timeout_uses_remaining_overall_budget(self):
        with patch.object(BARRIER.subprocess, "run", return_value=types.SimpleNamespace(stdout="ok")) as run:
            self.assertEqual(BARRIER.oc(7, "get", "nodes"), "ok")
            self.assertEqual(run.call_args.kwargs["timeout"], 7)
            self.assertIn("--request-timeout=60s", run.call_args.args[0])
        self.clock.now = 7
        with self.assertRaises(TimeoutError):
            BARRIER.oc(7, "get", "nodes")

    def test_request_timeout_resets_quiet_period(self):
        def snapshot(now):
            if now == 15:
                raise subprocess.TimeoutExpired(["oc", "get"], 60)

        self.assertTrue(self.wait(snapshot))
        self.assertEqual(self.clock.now, 60)

    def test_shell_wrapper_prints_diagnostics_and_fails(self):
        definitions = SCRIPT.read_text().split("# Bound diagnostics", 1)[1].split("# Get yq tool", 1)[0]
        definitions = "# Bound diagnostics" + definitions
        result = subprocess.run(["bash", "-c", "set -e\n" + definitions + '''
python3() { cat >/dev/null; return 1; }
oc() { echo "diagnostic: $*"; return 1; }
MGMT_CONVERGENCE_TIMEOUT=120
MGMT_CONVERGENCE_QUIET_PERIOD=30
MGMT_AUTOPILOT_MC_WAIT=60
MGMT_AUTOPILOT_MC_PATTERN='psi-karg|swap-online'
wait_for_management_nodes
echo "unexpected success"
'''], text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Error: management nodes did not converge", result.stdout)
        self.assertIn("MachineConfigPools:", result.stdout)
        self.assertIn("Nodes:", result.stdout)
        self.assertIn("machine-config-operator pods:", result.stdout)
        self.assertNotIn("unexpected success", result.stdout)


if __name__ == "__main__":
    unittest.main()
