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

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
short_sha="$(git -C "$repo_root" rev-parse --short=12 HEAD)"
image="${OMB_IMAGE:-nereus-benchmark/openmessaging-benchmark:pulsar-b${short_sha}-amd64}"
export NERDCTL_NAMESPACE="${NERDCTL_NAMESPACE:-k8s.io}"
export DOCKERFILE="${DOCKERFILE:-docker/Dockerfile.build}"
export CONTEXT="$repo_root"

"$repo_root/docker/build-containerd.sh" "$image"
inspection="$(nerdctl --namespace "$NERDCTL_NAMESPACE" image inspect --format '{{json .}}' "$image" 2>/dev/null || true)"
digest="$(python3 -c '
import json
import sys

try:
    value = json.load(sys.stdin)
    value = value[0] if isinstance(value, list) else value
    target = value.get("Target") or value.get("target") or {}
    digest = target.get("Digest", "")
    if not digest:
        for item in value.get("RepoDigests", value.get("repoDigests", [])):
            if "@" in item:
                digest = item.rsplit("@", 1)[1]
                break
    print(digest)
except (ValueError, AttributeError, TypeError):
    print("")
' <<<"$inspection")"
if [[ ! "$digest" =~ ^sha256:[0-9a-fA-F]{64}$ ]]; then
  echo "error: built image has no OCI target digest: $image" >&2
  exit 1
fi
printf 'IMAGE_REF=%s\nIMAGE_DIGEST=%s\nSOURCE_SHA=%s\n' "$image" "$digest" "$short_sha"
