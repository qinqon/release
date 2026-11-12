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


# Every oc call in the convergence barrier is bounded. Without this an apiserver
# that accepts connections but stops answering would hold a single query open
# past MGMT_CONVERGENCE_TIMEOUT, so the loop would never reach its deadline check
# and the timeout diagnostics would never print.
OC_REQUEST_TIMEOUT="60s"

# Fetch a listing as JSON. Returns non-zero on any failure, including a request
# that times out, so callers can tell "nothing is pending" from "state unknown".
# Empty output counts as a failure: jq reads it as an empty stream and exits 0,
# which would otherwise turn an unanswered query into "nothing is pending".
function oc_get_json() {
  local out
  if ! out="$(oc get "$@" -o json --request-timeout="${OC_REQUEST_TIMEOUT}")" || [[ -z "${out}" ]]; then
    return 1
  fi
  printf '%s\n' "${out}"
}

# Decide whether the MachineConfigPool API exists on this cluster. Prints
# "present" or "absent" and returns non-zero when neither could be established.
# A failed query is not evidence of an absent API: an RBAC denial, a 503 from an
# apiserver still digesting the CNV install, or a stale kubeconfig would
# otherwise disable the barrier and log a wrong cause for it.
function machineconfigpool_api_state() {
  local attempt output rc
  for attempt in 1 2 3; do
    output="$(oc api-resources --api-group=machineconfiguration.openshift.io -o name --request-timeout="${OC_REQUEST_TIMEOUT}" 2>/dev/null)" && rc=0 || rc=$?
    if grep -q '^machineconfigpools\.' <<<"${output}"; then
      echo present
      return 0
    fi
    if [[ ${rc} -eq 0 ]]; then
      echo absent
      return 0
    fi
    echo "Could not list the machineconfiguration.openshift.io API resources (attempt ${attempt}/3)" >&2
    if [[ ${attempt} -lt 3 ]]; then
      sleep 10
    fi
  done
  return 1
}

# Summarise the MachineConfigs that virt-platform-autopilot adds as a JSON object
# mapping MachineConfigPool role to the configs that target it, for example
# {"master":["99-openshift-machineconfig-master-psi-karg"]}. Empty until the
# autopilot has created them.
function autopilot_machineconfigs() {
  local mcs
  if ! mcs="$(oc_get_json machineconfig)"; then
    return 1
  fi
  jq -c --arg pattern "${MGMT_AUTOPILOT_MC_PATTERN}" '
    reduce (.items[]? | select(.metadata.name | test($pattern))) as $mc
      ({}; .[($mc.metadata.labels["machineconfiguration.openshift.io/role"] // "unknown")] += [$mc.metadata.name])
  ' <<<"${mcs}"
}

# Report every MachineConfigPool that is not fully rolled out, one per line.
# Prints nothing when every pool has converged. Takes the MachineConfigPool
# listing and the autopilot MachineConfig summary as JSON so that the caller owns
# query error handling: a pool list this function failed to read must not look
# like a pool list with nothing pending in it.
function unconverged_pools() {
  local pools_json="$1"
  local autopilot_json="$2"

  jq -r --argjson autopilot "${autopilot_json}" '
    .items[]
    | . as $p
    | (reduce (.status.conditions[]?) as $c ({}; .[$c.type] = $c.status)) as $cond
    | ($p.status.machineCount // -1) as $machines
    | ([$p.status.configuration.source[]?.name]) as $applied
    | (($autopilot[$p.metadata.name] // []) - $applied) as $missing
    | select(
        ($cond.Updated // "")        != "True"
        or ($cond.Updating // "")    == "True"
        or ($cond.Degraded // "")    == "True"
        or ($cond.NodeDegraded // "")   == "True"
        or ($cond.RenderDegraded // "") == "True"
        or $machines < 0
        or ($p.status.updatedMachineCount // -1) != $machines
        or ($p.status.readyMachineCount // -1)   != $machines
        or ($p.status.degradedMachineCount // -1) != 0
        or ($missing | length) > 0
      )
    | "  pool \($p.metadata.name): Updated=\($cond.Updated) Updating=\($cond.Updating) Degraded=\($cond.Degraded) updated=\($p.status.updatedMachineCount)/\($machines) ready=\($p.status.readyMachineCount) degraded=\($p.status.degradedMachineCount) config=\($p.status.configuration.name) unrenderedAutopilotConfigs=\(if ($missing | length) > 0 then ($missing | join(",")) else "none" end)"
  ' <<<"${pools_json}"
}

# Report every MCO-managed node that has not settled on its desired MachineConfig
# or is not Ready, one per line. Nodes the MCO does not manage are ignored.
# Takes the node listing as JSON for the same reason as unconverged_pools.
function unconverged_nodes() {
  local nodes_json="$1"

  jq -r '
    .items[]
    | . as $n
    | (.metadata.annotations // {}) as $a
    | select($a["machineconfiguration.openshift.io/state"] != null)
    | ($a["machineconfiguration.openshift.io/currentConfig"] // "") as $current
    | ($a["machineconfiguration.openshift.io/desiredConfig"] // "") as $desired
    | ($a["machineconfiguration.openshift.io/state"] // "") as $state
    | ([.status.conditions[]? | select(.type == "Ready") | .status] | first // "") as $ready
    | select($current == "" or $current != $desired or $state != "Done" or $ready != "True")
    | "  node \($n.metadata.name): current=\($current) desired=\($desired) state=\($state) Ready=\($ready)"
  ' <<<"${nodes_json}"
}

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

# Block until the management cluster's nodes have finished any MachineConfig
# rollout and are Ready again.
#
# virt-platform-autopilot, which ships with CNV, creates MachineConfigs that add
# kernel arguments (psi=1) and enable swap on the nodes it manages. Those configs
# trigger an MCO rollout that reboots every node in the affected pools. The
# HyperConverged Available condition says nothing about that rollout, so without
# this barrier the step returns successfully while the management nodes are still
# rebooting and the e2e test starts against a cluster that is about to lose them.
# Masters are schedulable in this topology (see the mastersSchedulable patch
# above) and host hosted control plane pods including etcd, so waiting on the
# worker pool alone is not sufficient.
#
# A single Updated=True check is also not sufficient: it can observe a pool before
# the MCO has rendered the autopilot MachineConfigs, in which case the pool still
# reports the *previous* rollout as complete. Two things guard against that. The
# positive one is the autopilot MachineConfigs themselves: once they exist, every
# pool they target must list them in status.configuration.source before it counts
# as converged, so a pool cannot pass on a rendered config that predates them.
# The fallback is time: the barrier does not accept the cluster until the
# converged state has held for a quiet period and the autopilot has had
# MGMT_AUTOPILOT_MC_WAIT seconds to create those MachineConfigs at all. A CNV
# version that installs none therefore costs the longer of the two windows, not
# their sum.
#
# A failed query is never convergence. Every read is checked, and a cluster whose
# MachineConfig state could not be read is treated as not converged, because an
# apiserver blip is most likely exactly when a node is rebooting.
function wait_for_management_nodes() {
  local started=${SECONDS}
  local deadline=$(( SECONDS + MGMT_CONVERGENCE_TIMEOUT ))
  local stable_since=-1
  local rc=1
  local api_state autopilot autopilot_at=-1 autopilot_seen=false
  local pools_json nodes_json pools_pending nodes_pending pending
  local last_report="" polls_since_report=0

  if ! api_state="$(machineconfigpool_api_state)"; then
    echo "Error: could not determine whether this cluster has a MachineConfigPool API"
    return 1
  fi
  if [[ "${api_state}" == "absent" ]]; then
    echo "No MachineConfigPool API on this cluster, skipping management node convergence barrier"
    return 0
  fi

  echo "Waiting up to ${MGMT_CONVERGENCE_TIMEOUT}s for management nodes to converge on their MachineConfigs"
  # The poll runs for many iterations and is unreadable under xtrace.
  local restore_xtrace=false
  if [[ $- == *x* ]]; then
    restore_xtrace=true
    set +x
  fi

  while [[ ${SECONDS} -lt ${deadline} ]]; do
    # The MachineConfig listing carries every rendered config, so refresh the
    # autopilot summary on its own slower cadence rather than every poll.
    if [[ ${autopilot_at} -lt 0 || $(( SECONDS - autopilot_at )) -ge 60 ]]; then
      if ! autopilot="$(autopilot_machineconfigs)"; then
        echo "Warning: could not read the cluster's MachineConfigs, treating the management nodes as not converged"
        stable_since=-1
        sleep 15
        continue
      fi
      autopilot_at=${SECONDS}
      if [[ "${autopilot_seen}" == false && "${autopilot}" != "{}" ]]; then
        autopilot_seen=true
        echo "virt-platform-autopilot MachineConfigs are present: ${autopilot}"
      fi
    fi

    if ! pools_json="$(oc_get_json machineconfigpool)" || ! nodes_json="$(oc_get_json nodes)"; then
      echo "Warning: could not read the cluster's MachineConfigPools or nodes, treating the management nodes as not converged"
      stable_since=-1
      sleep 15
      continue
    fi

    if ! pools_pending="$(unconverged_pools "${pools_json}" "${autopilot}")" || ! nodes_pending="$(unconverged_nodes "${nodes_json}")"; then
      echo "Warning: could not evaluate the cluster's MachineConfig state, treating the management nodes as not converged"
      stable_since=-1
      sleep 15
      continue
    fi

    pending="${pools_pending}"
    if [[ -n "${nodes_pending}" ]]; then
      if [[ -n "${pending}" ]]; then
        pending+=$'\n'
      fi
      pending+="${nodes_pending}"
    fi

    if [[ -z "${pending}" ]]; then
      if [[ ${stable_since} -lt 0 ]]; then
        stable_since=${SECONDS}
        echo "Management nodes look converged, requiring that to hold for ${MGMT_CONVERGENCE_QUIET_PERIOD}s"
      fi
      local elapsed=$(( SECONDS - started ))
      if [[ "${autopilot_seen}" == false && ${elapsed} -lt ${MGMT_AUTOPILOT_MC_WAIT} ]]; then
        if [[ ${polls_since_report} -eq 0 ]]; then
          echo "No virt-platform-autopilot MachineConfigs yet, holding for up to $(( MGMT_AUTOPILOT_MC_WAIT - elapsed ))s more in case the autopilot is still creating them"
        fi
        polls_since_report=$(( (polls_since_report + 1) % 8 ))
      elif [[ $(( SECONDS - stable_since )) -ge ${MGMT_CONVERGENCE_QUIET_PERIOD} ]]; then
        echo "Management nodes stayed converged for ${MGMT_CONVERGENCE_QUIET_PERIOD}s"
        rc=0
        break
      fi
    else
      if [[ ${stable_since} -ge 0 ]]; then
        echo "A MachineConfig rollout started while waiting, restarting the quiet period"
        stable_since=-1
      fi
      # Over a long rollout this block is otherwise reprinted every 15s. Log it
      # when it changes, and every eighth poll so the job does not look stuck.
      if [[ "${pending}" != "${last_report}" || ${polls_since_report} -eq 0 ]]; then
        echo "Still waiting on:"
        echo "${pending}"
        last_report="${pending}"
        polls_since_report=1
      else
        polls_since_report=$(( (polls_since_report + 1) % 8 ))
      fi
    fi
    sleep 15
  done

  if [[ ${rc} -ne 0 ]]; then
    echo "Error: management nodes did not converge within ${MGMT_CONVERGENCE_TIMEOUT}s"
    machineconfig_diagnostics
  fi

  if [[ ${restore_xtrace} == true ]]; then
    set -x
  fi
  return ${rc}
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
