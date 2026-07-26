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

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 DRIVER_YAML [DEPLOYMENT_RUN_ENV]" >&2
  exit 2
fi

driver_yaml="$1"
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
results_root="${NEREUS_RESULTS_ROOT:-${repo_root}/results}"
run_env="${2:-${NEREUS_RUN_ENV:-${results_root}/deploy/latest.env}}"

[[ -r "$driver_yaml" ]] || { echo "deployment preflight: driver is not readable: $driver_yaml" >&2; exit 1; }
[[ -r "$run_env" ]] || { echo "deployment preflight: run file is not readable: $run_env" >&2; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "deployment preflight: kubectl is required" >&2; exit 1; }

# shellcheck disable=SC1090
source "$run_env"

for name in STAGE RELEASE KUBERNETES_NAMESPACE KUBERNETES_CONTEXT CLUSTER RUN_DIR; do
  [[ -n "${!name:-}" ]] || {
    echo "deployment preflight: $run_env does not define $name" >&2
    exit 1
  }
done

current_context="$(kubectl config current-context)"
[[ "$current_context" == "$KUBERNETES_CONTEXT" ]] || {
  echo "deployment preflight: Kubernetes context mismatch: expected=$KUBERNETES_CONTEXT actual=$current_context" >&2
  exit 1
}

if [[ -n "${NEREUS_NAMESPACE:-}" && "$NEREUS_NAMESPACE" != "$KUBERNETES_NAMESPACE" ]]; then
  echo "deployment preflight: NEREUS_NAMESPACE does not match deployment run" >&2
  exit 1
fi
if [[ -n "${NEREUS_RELEASE:-}" && "$NEREUS_RELEASE" != "$RELEASE" ]]; then
  echo "deployment preflight: NEREUS_RELEASE does not match deployment run" >&2
  exit 1
fi

python3 - "$driver_yaml" "$STAGE" "$CLUSTER" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
stage = sys.argv[2]
cluster = sys.argv[3]
text = path.read_text()
if any(token in text for token in ("<FINAL_", "${", "n00000000", "p00000000")):
    raise SystemExit("driver contains unresolved deployment placeholders")

def field(name):
    match = re.search(rf"^\s+{re.escape(name)}:\s*([^#\s]+)", text, re.MULTILINE)
    if not match:
        return None
    return match.group(1).strip("'\"")

actual_stage = field("stage")
storage = field("managedLedgerStorageClassName")
run_id = field("runId")
namespace_suffix = field("namespaceSuffix")
configured_cluster = field("clusterName")
if actual_stage != stage:
    raise SystemExit(f"driver stage {actual_stage!r} does not match deployment stage {stage!r}")
if run_id != namespace_suffix:
    raise SystemExit("driver runId must equal client.namespaceSuffix")
expected_storage = "bookkeeper" if stage in ("A", "B") else "nereus"
if storage != expected_storage:
    raise SystemExit(f"stage {stage} requires storage class {expected_storage}, got {storage!r}")
if configured_cluster != cluster:
    raise SystemExit(
        f"driver clusterName {configured_cluster!r} does not match deployment cluster {cluster!r}"
    )
PY

for image_id in APACHE_IMAGE_CONFIG_ID NEREUS_IMAGE_CONFIG_ID NEREUS_ADMIN_IMAGE_CONFIG_ID; do
  if [[ "$STAGE" != "A" || "$image_id" == "APACHE_IMAGE_CONFIG_ID" ]]; then
    [[ "${!image_id:-}" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || {
      echo "deployment preflight: $image_id is not an immutable runtime config ID" >&2
      exit 1
    }
  fi
done

printf 'deployment preflight passed: stage=%s release=%s namespace=%s cluster=%s runEnv=%s\n' \
  "$STAGE" "$RELEASE" "$KUBERNETES_NAMESPACE" "$CLUSTER" "$run_env"
