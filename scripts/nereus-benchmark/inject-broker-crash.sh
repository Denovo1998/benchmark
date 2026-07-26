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

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "usage: $0 BROKER_POD [NAMESPACE] [EVENTS_JSONL]" >&2
  exit 2
fi
pod="$1"
namespace="${2:-pulsar}"
events_file="${3:-fault-events.jsonl}"
command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; exit 1; }

selector="${BROKER_SELECTOR:-release=${NEREUS_RELEASE:-nereus},component=broker}"
timeout_seconds="${R1_RECOVERY_TIMEOUT_SECONDS:-600}"
[[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || { echo "invalid R1_RECOVERY_TIMEOUT_SECONDS" >&2; exit 2; }
mkdir -p "$(dirname "$events_file")"

before_json="$(kubectl -n "$namespace" get pod "$pod" -o json)"
before_start_time="$(jq -r '.status.startTime // ""' <<<"$before_json")"
request_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kubectl -n "$namespace" delete pod "$pod" --grace-period=0 --force

deleted_at=""
for ((i = 0; i < timeout_seconds; i++)); do
  if ! kubectl -n "$namespace" get pod "$pod" -o json >/dev/null 2>&1; then
    deleted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    break
  fi
  sleep 1
done

replacement_pod=""
replacement_ready_at=""
for ((i = 0; i < timeout_seconds; i++)); do
  replacement_pod="$(kubectl -n "$namespace" get pods -l "$selector" -o json \
    | jq -r --arg pod "$pod" --arg start "$before_start_time" '
        [.items[]
          | select(.status.phase == "Running")
          | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
          | select(.metadata.name != $pod or (.status.startTime // "") != $start)
          | .metadata.name]
        | sort | .[0] // ""')"
  if [[ -n "$replacement_pod" ]]; then
    replacement_ready_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    break
  fi
  sleep 1
done

owner_before='{}'
owner_after='{}'
if [[ -n "${R1_OWNER_MAP_BEFORE:-}" && -r "${R1_OWNER_MAP_BEFORE}" ]]; then
  owner_before="$(jq -c . "$R1_OWNER_MAP_BEFORE")"
fi
if [[ -n "${R1_OWNER_MAP_AFTER:-}" && -r "${R1_OWNER_MAP_AFTER}" ]]; then
  owner_after="$(jq -c . "$R1_OWNER_MAP_AFTER")"
fi
jq -cn \
  --arg requestTime "$request_time" \
  --arg deletedAt "$deleted_at" \
  --arg replacementReadyAt "$replacement_ready_at" \
  --arg pod "$pod" \
  --arg replacementPod "$replacement_pod" \
  --arg namespace "$namespace" \
  --arg selector "$selector" \
  --argjson ownerMapBefore "$owner_before" \
  --argjson ownerMapAfter "$owner_after" \
  '{type:"broker-crash", requestTime:$requestTime, podDeletedAt:$deletedAt,
    replacementReadyAt:$replacementReadyAt, pod:$pod, replacementPod:$replacementPod,
    namespace:$namespace, brokerSelector:$selector,
    ownerMapBefore:$ownerMapBefore, ownerMapAfter:$ownerMapAfter}' \
  >> "$events_file"

[[ -n "$deleted_at" ]] || { echo "broker pod deletion was not observed before timeout" >&2; exit 1; }
[[ -n "$replacement_ready_at" ]] || { echo "replacement broker was not Ready before timeout" >&2; exit 1; }
echo "injected broker crash for $namespace/$pod; replacement=$replacement_pod"
