#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "usage: $0 DRIVER_YAML WORKLOAD_YAML [WORKERS_FILE] [OUTPUT]" >&2
  exit 2
fi

driver_yaml="$1"
workload_yaml="$2"
workers_file="${3:-}"
output="${4:-}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
"$script_dir/validate-run-config.sh" "$driver_yaml"

if grep -Eq '^[[:space:]]*run:[[:space:]]*$' "$driver_yaml"; then
  deployment_run_env="${NEREUS_RUN_ENV:-${NEREUS_RESULTS_ROOT:-$repo_root/results}/deploy/latest.env}"
  bash "$script_dir/preflight-deployment.sh" "$driver_yaml" "$deployment_run_env"
  # run.env is produced by the Helm deployment script and contains only public
  # identity/evidence paths. Export its values so Benchmark can bind the run
  # manifest to the exact deployment that was tested.
  # shellcheck disable=SC1090
  source "$deployment_run_env"
  export OMB_DEPLOYMENT_RUN_ENV="$deployment_run_env"
  export OMB_DEPLOYMENT_RUN_ENV_SHA256="$(sha256sum "$deployment_run_env" | awk '{print $1}')"
  export OMB_PULSAR_STAGE="${STAGE}"
  export OMB_PULSAR_RELEASE="${RELEASE}"
  export OMB_KUBERNETES_CONTEXT="${KUBERNETES_CONTEXT}"
  export OMB_KUBERNETES_NAMESPACE="${KUBERNETES_NAMESPACE}"
  export OMB_PULSAR_CLUSTER="${CLUSTER}"
  if [[ "${STAGE}" == "A" ]]; then
    # Stage A runs the Apache baseline broker; deployment env also carries the
    # frozen Nereus image identity for the later stages.
    export OMB_PULSAR_BROKER_IMAGE_ID="${APACHE_IMAGE_TARGET_DIGEST:-}"
    export OMB_NEREUS_SOURCE_ID=""
  else
    export OMB_PULSAR_BROKER_IMAGE_ID="${NEREUS_IMAGE_TARGET_DIGEST:-}"
    export OMB_NEREUS_SOURCE_ID="${NEREUS_IMAGE_TARGET_DIGEST:-}"
  fi
  if [[ -z "${OMB_HELM_EVIDENCE_ARCHIVE:-}" && -n "${NEREUS_HELM_EVIDENCE_ARCHIVE:-}" ]]; then
    export OMB_HELM_EVIDENCE_ARCHIVE="$NEREUS_HELM_EVIDENCE_ARCHIVE"
  fi
  if [[ -n "${OMB_HELM_EVIDENCE_ARCHIVE:-}" && -r "${OMB_HELM_EVIDENCE_ARCHIVE}" ]]; then
    export OMB_HELM_EVIDENCE_ARCHIVE_SHA256="$(sha256sum "$OMB_HELM_EVIDENCE_ARCHIVE" | awk '{print $1}')"
  fi
fi

args=(--drivers "$driver_yaml")
if [[ -n "$workers_file" ]]; then
  args+=(--workers-file "$workers_file")
fi
if [[ -n "$output" ]]; then
  args+=(--output "$output")
fi
args+=("$workload_yaml")

exec "$repo_root/bin/benchmark" "${args[@]}"
