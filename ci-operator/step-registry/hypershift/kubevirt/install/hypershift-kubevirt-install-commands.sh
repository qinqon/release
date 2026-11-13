#!/usr/bin/env bash

set -ex

function ocp_version() {
    oc get clusterversion version -o jsonpath='{.status.desired.version}' | awk -F "." '{print $1"."$2}'
}
if [[ ! "${CNV_SUBSCRIPTION_SOURCE}" =~ ^(cnv-prerelease-catalog-source|redhat-operators)$ ]]
then
    echo "CNV_SUBSCRIPTION_SOURCE environment variable value '${CNV_SUBSCRIPTION_SOURCE}' not allowed, allowed values are 'redhat-operators' or 'cnv-prerelease-catalog-source'"
    exit 1
fi

# Reject a convergence barrier that can never succeed before the step gets as
# far as waiting on it.
for setting in MGMT_CONVERGENCE_TIMEOUT MGMT_CONVERGENCE_QUIET_PERIOD MGMT_AUTOPILOT_MC_WAIT
do
    if [[ ! "${!setting}" =~ ^[0-9]+$ ]]
    then
        echo "${setting} environment variable value '${!setting}' is not a whole number of seconds"
        exit 1
    fi
done
if [[ ${MGMT_CONVERGENCE_QUIET_PERIOD} -ge ${MGMT_CONVERGENCE_TIMEOUT} || ${MGMT_AUTOPILOT_MC_WAIT} -ge ${MGMT_CONVERGENCE_TIMEOUT} ]]
then
    echo "MGMT_CONVERGENCE_QUIET_PERIOD (${MGMT_CONVERGENCE_QUIET_PERIOD}) and MGMT_AUTOPILOT_MC_WAIT (${MGMT_AUTOPILOT_MC_WAIT}) must each be smaller than MGMT_CONVERGENCE_TIMEOUT (${MGMT_CONVERGENCE_TIMEOUT})"
    exit 1
fi


function add_pullsecret() {
  local REGISTRY=$1
  local USERNAME=$2
  local PASSWORD=$3

  oc extract secret/pull-secret -n openshift-config --keys=.dockerconfigjson --to=/tmp --confirm

  jq --arg reg "$REGISTRY" --arg user "$USERNAME" --arg pass "$PASSWORD" \
     '.auths[$reg] = {auth: ($user + ":" + $pass | @base64)}' \
     /tmp/.dockerconfigjson > /tmp/.dockerconfigjson.new

  oc set data secret/pull-secret -n openshift-config --from-file=.dockerconfigjson=/tmp/.dockerconfigjson.new

  rm /tmp/.dockerconfigjson /tmp/.dockerconfigjson.new
}


# Bound diagnostics too, so an unavailable API cannot prevent the step exiting.
OC_REQUEST_TIMEOUT="60s"

function machineconfig_diagnostics() {
  echo "MachineConfigPools:"
  oc get machineconfigpool -o wide --request-timeout="${OC_REQUEST_TIMEOUT}" || true
  echo "Nodes:"
  oc get nodes -o wide --request-timeout="${OC_REQUEST_TIMEOUT}" || true
  echo "Node MachineConfig annotations:"
  oc get nodes --request-timeout="${OC_REQUEST_TIMEOUT}" -o custom-columns=\
'NAME:.metadata.name,'\
'CURRENT:.metadata.annotations.machineconfiguration\.openshift\.io/currentConfig,'\
'DESIRED:.metadata.annotations.machineconfiguration\.openshift\.io/desiredConfig,'\
'STATE:.metadata.annotations.machineconfiguration\.openshift\.io/state,'\
'REASON:.metadata.annotations.machineconfiguration\.openshift\.io/reason' || true
  echo "machine-config-operator pods:"
  oc get pods -n openshift-machine-config-operator -o wide --request-timeout="${OC_REQUEST_TIMEOUT}" || true
  echo "virt-platform-autopilot MachineConfigs:"
  oc get machineconfig --request-timeout="${OC_REQUEST_TIMEOUT}" 2>/dev/null | grep -E "${MGMT_AUTOPILOT_MC_PATTERN}" || echo "  none matching ${MGMT_AUTOPILOT_MC_PATTERN}"
}

# HCO Available does not cover autopilot's MachineConfigs or the reboots they
# trigger. Wait for their application in every selected pool (including masters)
# and require a continuously healthy polling window. Python is already provided
# by upi-installer; keep JSON checks as ordinary functions rather than jq programs.
function wait_for_management_nodes() {
  if python3 - "${MGMT_CONVERGENCE_TIMEOUT}" "${MGMT_CONVERGENCE_QUIET_PERIOD}" \
    "${MGMT_AUTOPILOT_MC_WAIT}" "${MGMT_AUTOPILOT_MC_PATTERN}" <<'PY'
import json
import re
import subprocess
import sys
import time


def oc(deadline, *args):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("management node convergence deadline reached")
    return subprocess.run(
        ["oc", *args, "--request-timeout=60s"], check=True,
        stdout=subprocess.PIPE, text=True, timeout=min(60, remaining),
    ).stdout


def get_items(deadline, resource):
    # Empty/malformed output is an error, not an empty list of pending work.
    items = json.loads(oc(deadline, "get", resource, "-o", "json"))["items"]
    if not isinstance(items, list) or (not items and resource != "machineconfig"):
        raise ValueError(f"invalid or empty {resource} listing")
    return items


def selector_matches(selector, labels):
    if selector is None:
        return False
    if any(labels.get(key) != value for key, value in selector.get("matchLabels", {}).items()):
        return False
    for expression in selector.get("matchExpressions", []):
        key, operator = expression["key"], expression["operator"]
        values = expression.get("values", [])
        if operator == "In":
            matches = key in labels and labels[key] in values
        elif operator == "NotIn":
            matches = key not in labels or labels[key] not in values
        elif operator == "Exists":
            matches = key in labels
        elif operator == "DoesNotExist":
            matches = key not in labels
        else:
            raise ValueError(f"unsupported selector operator: {operator}")
        if not matches:
            return False
    return True


def conditions(resource):
    return {entry["type"]: entry["status"] for entry in resource.get("status", {}).get("conditions", [])}


def pending_pools(pools, configs):
    pending = []
    for pool in pools:
        status, cond = pool.get("status", {}), conditions(pool)
        spec = pool.get("spec", {})
        machines = status.get("machineCount", -1)
        applied_config = status.get("configuration", {})
        applied = {source["name"] for source in applied_config.get("source", [])}
        selector = spec.get("machineConfigSelector")
        missing = [mc["metadata"]["name"] for mc in configs
                   if selector_matches(selector, mc["metadata"].get("labels", {}))
                   and mc["metadata"]["name"] not in applied]
        healthy = (
            cond.get("Updated") == "True"
            and status.get("observedGeneration", -1) >= pool["metadata"].get("generation", 0)
            and applied_config.get("name") == spec.get("configuration", {}).get("name")
            and not any(cond.get(key) == "True" for key in
                        ("Updating", "Degraded", "NodeDegraded", "RenderDegraded"))
            and machines >= 0
            and status.get("updatedMachineCount") == machines
            and status.get("readyMachineCount") == machines
            and status.get("degradedMachineCount") == 0
        )
        if not healthy or missing:
            pending.append(f"pool {pool['metadata']['name']}: not converged; unapplied configs={missing}")
    return pending


def pending_nodes(nodes):
    pending = []
    prefix = "machineconfiguration.openshift.io/"
    for node in nodes:
        annotations = node["metadata"].get("annotations", {})
        if prefix + "state" not in annotations:
            continue  # Not managed by MCO.
        current = annotations.get(prefix + "currentConfig")
        desired = annotations.get(prefix + "desiredConfig")
        state, ready = annotations[prefix + "state"], conditions(node).get("Ready")
        if not current or current != desired or state != "Done" or ready != "True":
            pending.append(f"node {node['metadata']['name']}: current={current} desired={desired} state={state} Ready={ready}")
    return pending


def wait_for_nodes(timeout, quiet_period, discovery_wait, pattern):
    pattern = re.compile(pattern)
    started = time.monotonic()
    deadline = started + timeout
    # Only a successful discovery response may establish that MCPs are absent.
    for attempt in range(3):
        try:
            resources = oc(deadline, "api-resources", "--api-group=machineconfiguration.openshift.io", "-o", "name")
            if not any(line.startswith("machineconfigpools.") for line in resources.splitlines()):
                print("No MachineConfigPool API; skipping management node convergence barrier", flush=True)
                return True
            break
        except (subprocess.SubprocessError, TimeoutError):
            if attempt == 2 or time.monotonic() >= deadline:
                raise
            time.sleep(min(10, max(0, deadline - time.monotonic())))

    stable_since = None
    last_report, next_report = None, started
    print(f"Waiting up to {timeout}s for management nodes; requiring {quiet_period}s stable", flush=True)
    while time.monotonic() < deadline:
        try:
            # Refresh discovery every poll so a later MC cannot hide behind a cache.
            configs = [mc for mc in get_items(deadline, "machineconfig")
                       if pattern.search(mc["metadata"]["name"])]
            pools = get_items(deadline, "machineconfigpool")
            nodes = get_items(deadline, "nodes")
            pending = pending_pools(pools, configs) + pending_nodes(nodes)
        except (subprocess.SubprocessError, TimeoutError, ValueError, KeyError) as error:
            pending = [f"could not read/evaluate MachineConfig state: {error}"]

        now = time.monotonic()
        if now >= deadline:
            break
        if pending:
            stable_since = None
            report = "Still waiting on:\n  " + "\n  ".join(pending)
        else:
            if stable_since is None:
                stable_since = now
            # Versions without autopilot MCs pay max(discovery_wait, quiet_period),
            # not their sum. Known unapplied MCs never get this fallback.
            discovery_done = bool(configs) or now - started >= discovery_wait
            if discovery_done and now - stable_since >= quiet_period:
                print(f"Management nodes stayed converged for {quiet_period}s", flush=True)
                return True
            report = "Management nodes look converged; waiting for discovery/quiet period"
        if report != last_report or now >= next_report:
            print(report, flush=True)
            last_report, next_report = report, now + 120
        time.sleep(min(15, max(0, deadline - time.monotonic())))
    return False


if __name__ == "__main__":
    try:
        success = wait_for_nodes(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
    except (subprocess.SubprocessError, TimeoutError, ValueError, re.error) as error:
        print(f"Error: {error}", file=sys.stderr)
        success = False
    sys.exit(0 if success else 1)
PY
  then
    return 0
  fi
  echo "Error: management nodes did not converge within ${MGMT_CONVERGENCE_TIMEOUT}s"
  machineconfig_diagnostics
  return 1
}


# Get yq tool
YQ="/tmp/yq"
curl -L -o ${YQ} https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64
chmod +x ${YQ}

# Dynamically get CNV catalog image and channel that were provided to the job via gangway API
CNV_PRERELEASE_CATALOG_IMAGE=$(curl -s https://prow.ci.openshift.org/prowjob?prowjob="${PROW_JOB_ID}" |\
  ${YQ} e '.spec.pod_spec.containers[0].env[] | select(.name == "CNV_PRERELEASE_CATALOG_IMAGE") | .value')
CNV_SUBSCRIPTION_CHANNEL=$(curl -s https://prow.ci.openshift.org/prowjob?prowjob="${PROW_JOB_ID}" |\
  ${YQ} e '.spec.pod_spec.containers[0].env[] | select(.name == "CNV_CHANNEL") | .value')

if [ "${CNV_SUBSCRIPTION_SOURCE}" == "redhat-operators" ]
then
  CNV_RELEASE_CHANNEL=stable
elif [ -n "${CNV_PRERELEASE_CATALOG_IMAGE}" ] && [ -n "${CNV_SUBSCRIPTION_CHANNEL}" ]
then
  CNV_RELEASE_CHANNEL=${CNV_SUBSCRIPTION_CHANNEL}
else
  CNV_RELEASE_CHANNEL=nightly-$(ocp_version)
  CNV_PRERELEASE_CATALOG_IMAGE=quay.io/openshift-cnv/nightly-catalog:$(ocp_version)
fi

# The kubevirt tests require wildcard routes to be allowed
oc patch ingresscontroller -n openshift-ingress-operator default --type=json -p '[{ "op": "add", "path": "/spec/routeAdmission", "value": {wildcardPolicy: "WildcardsAllowed"}}]'

# Make the masters schedulable so we have more capacity to run VMs
CONTROL_PLANE_TOPOLOGY=$(oc get infrastructure cluster -o jsonpath='{.status.controlPlaneTopology}')
if [[ ${CONTROL_PLANE_TOPOLOGY} != "External" ]]
then
  oc patch scheduler cluster --type=json -p '[{ "op": "replace", "path": "/spec/mastersSchedulable", "value": true }]'
fi

# In case of nested-mgmt topology where there's only one worker node, we need to label it as a master
# in order to get some kubevirt components to be scheduled. This is needed since CNV 4.17.0+
if [[ $(oc get nodes --no-headers | wc -l) -eq 1 ]]
then
	NODENAME=$(oc get nodes -o jsonpath='{.items[].metadata.name}')
	oc label node ${NODENAME} node-role.kubernetes.io/control-plane=
fi

if [ -n "${CNV_PRERELEASE_CATALOG_IMAGE}" ]
then
  if [[ "${CNV_PRERELEASE_CATALOG_IMAGE}" == *"brew"* ]]; then
    # Add brew registry pull secret
    add_pullsecret "brew.registry.redhat.io" "${BREW_IMAGE_REGISTRY_USERNAME}" "$(cat "${BREW_IMAGE_REGISTRY_TOKEN_PATH}")"

    # Deploy IDMS for brew registry
    cat <<EOF | oc apply -f -
apiVersion: config.openshift.io/v1
kind: ImageDigestMirrorSet
metadata:
  name: brew-idms
spec:
  imageDigestMirrors:
  - mirrors:
    - brew.registry.redhat.io
    source: registry.redhat.io
EOF
  elif [[ "${CNV_PRERELEASE_CATALOG_IMAGE}" == *"quay"* ]]; then
    # Add quay registry pull secret for cnv nightly channel
    QUAY_USERNAME=openshift-cnv+openshift_ci
    QUAY_PASSWORD=$(cat /etc/cnv-nightly-pull-credentials/openshift_cnv_pullsecret)
    add_pullsecret "quay.io/openshift-cnv" "${QUAY_USERNAME}" "${QUAY_PASSWORD}"
  fi

  # Wait for MCPs to reach stable state after pull secret change
  # MCPs may already be updated or may need to update - just ensure they're stable
  oc wait mcp master worker --for condition=Updated=True --timeout=20m

  # Create a catalog source for the pre-release builds
  cat <<EOF | oc apply -f -
apiVersion: operators.coreos.com/v1alpha1
kind: CatalogSource
metadata:
  name: cnv-prerelease-catalog-source
  namespace: openshift-marketplace
spec:
  sourceType: grpc
  image: ${CNV_PRERELEASE_CATALOG_IMAGE}
  displayName: OpenShift Virtualization Pre-Release Catalog
  publisher: Red Hat
  updateStrategy:
    registryPoll:
      interval: 8h
EOF
fi

oc apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: openshift-cnv
EOF

oc apply -f - <<EOF
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: openshift-cnv-group
  namespace: openshift-cnv
spec:
  targetNamespaces:
  - openshift-cnv
EOF

cat <<EOF | oc apply -f -
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  labels:
    operators.coreos.com/kubevirt-hyperconverged.openshift-cnv: ''
  name: kubevirt-hyperconverged
  namespace: openshift-cnv
spec:
  channel: ${CNV_RELEASE_CHANNEL}
  installPlanApproval: Automatic
  name: kubevirt-hyperconverged
  source: ${CNV_SUBSCRIPTION_SOURCE}
  sourceNamespace: openshift-marketplace
EOF

sleep 30

RETRIES=30
CSV=
for i in $(seq ${RETRIES}); do
  if [[ -z ${CSV} ]]; then
    CSV=$(oc get subscription -n openshift-cnv kubevirt-hyperconverged -o jsonpath='{.status.installedCSV}')
  fi
  if [[ -z ${CSV} ]]; then
    echo "Try ${i}/${RETRIES}: can't get the CSV yet. Checking again in 30 seconds"
    sleep 30
  fi
  if [[ $(oc get csv -n openshift-cnv ${CSV} -o jsonpath='{.status.phase}') == "Succeeded" ]]; then
    echo "CNV is deployed"
    break
  else
    echo "Try ${i}/${RETRIES}: CNV is not deployed yet. Checking again in 30 seconds"
    sleep 30
  fi
done

if [[ $(oc get csv -n openshift-cnv ${CSV} -o jsonpath='{.status.phase}') != "Succeeded" ]]; then
  echo "Error: Failed to deploy CNV"
  echo "CSV ${CSV} YAML"
  oc get CSV ${CSV} -n openshift-cnv -o yaml
  echo
  echo "CSV ${CSV} Describe"
  oc describe CSV ${CSV} -n openshift-cnv
  exit 1
fi

# Deploy HyperConverged custom resource to complete kubevirt's installation
oc create -f - <<EOF
apiVersion: hco.kubevirt.io/v1beta1
kind: HyperConverged
metadata:
  name: kubevirt-hyperconverged
  namespace: openshift-cnv
spec:
  featureGates:
    enableCommonBootImageImport: false
  logVerbosityConfig:
    kubevirt:
      virtLauncher: 8
      virtHandler: 8
      virtController: 8
      virtApi: 8
      virtOperator: 8
EOF

oc wait hyperconverged -n openshift-cnv kubevirt-hyperconverged --for=condition=Available --timeout=15m

# CNV is installed, but the MachineConfig rollout it triggers may still be in
# flight. Do not let the step report success until the management nodes are back.
wait_for_management_nodes

# CDI auto-detects only volumeMode=Block for gp3-csi in its StorageProfile.
# KubeVirt DataVolumes created by HyperShift request volumeMode=Filesystem,
# which CDI rejects because no matching claimPropertySet exists.
# Patch the StorageProfile to also advertise Filesystem so DataVolumes can bind.
if oc get storageprofile gp3-csi &>/dev/null; then
  echo "Patching gp3-csi StorageProfile to include Filesystem volumeMode"
  oc patch storageprofile gp3-csi --type=merge -p \
    '{"spec":{"claimPropertySets":[{"accessModes":["ReadWriteOnce"],"volumeMode":"Filesystem"},{"accessModes":["ReadWriteOnce"],"volumeMode":"Block"}]}}'
fi

echo "Installing VM console logger in order to aid debugging potential VM boot issues"
oc apply -f https://raw.githubusercontent.com/davidvossel/kubevirt-console-debugger/main/kubevirt-console-logger.yaml


if [ "$(oc get infrastructure cluster -o=jsonpath='{.status.platformStatus.type}')" == "Azure" ];
then
  # Pin cpuModel to Broadwell in case of Azure cluster, to avoid discrepancies between the cluster nodes
  PATCH_COMMAND="oc patch hco kubevirt-hyperconverged -n openshift-cnv --type=json -p='[{\"op\": \"add\", \"path\": \"/spec/defaultCPUModel\", \"value\": \"Broadwell\"}]'"
  MAX_RETRIES=5
  for ((i=1; i<=MAX_RETRIES; i++)); do
    echo "Attempt $i of $MAX_RETRIES..."
    if eval $PATCH_COMMAND; then
      echo "Patch succeeded."
      exit 0
    else
      echo "Patch failed. Retrying in 2 seconds..."
      sleep 2
    fi
  done

  echo "Patch failed after $MAX_RETRIES attempts."
  exit 1
fi
