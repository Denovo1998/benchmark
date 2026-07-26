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

namespace="${CONTAINERD_NAMESPACE:-k8s.io}"
use_sudo="${CONTAINERD_USE_SUDO:-false}"
run_ctr() {
  if [[ "$use_sudo" == "true" ]]; then sudo ctr "$@"; else ctr "$@"; fi
}

image_digest() {
  local image="$1"
  local inspection
  inspection="$(nerdctl --namespace "$namespace" image inspect --format '{{json .}}' "$image" 2>/dev/null || true)"
  python3 -c '
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
' <<<"$inspection"
}

if [[ "$#" -lt 2 ]]; then
  echo "usage: $0 save IMAGE_REF OUTPUT_TAR | load INPUT_TAR [ENV_FILE]" >&2
  exit 2
fi
case "$1" in
  save)
    [[ "$#" -eq 3 ]] || { echo "save requires IMAGE_REF and OUTPUT_TAR" >&2; exit 2; }
    image="$2"
    output="$3"
    nerdctl --namespace "$namespace" save -o "$output" "$image"
    sha256sum "$output" > "${output}.sha256"
    digest="$(image_digest "$image")"
    [[ "$digest" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || {
      echo "error: image has no OCI target digest: $image" >&2
      exit 1
    }
    printf 'IMAGE_REF=%s\nIMAGE_DIGEST=%s\n' "$image" "$digest" > "${output}.env"
    ;;
  load)
    input="$2"
    env_file="${3:-${input}.env}"
    [[ -r "${input}.sha256" ]] || { echo "error: checksum sidecar not found: ${input}.sha256" >&2; exit 1; }
    sha256sum -c "${input}.sha256"
    run_ctr -n "$namespace" images import "$input"
    if [[ -r "$env_file" ]]; then
      image_ref="$(sed -n 's/^IMAGE_REF=//p' "$env_file" | head -n 1)"
      expected="$(sed -n 's/^IMAGE_DIGEST=//p' "$env_file" | head -n 1)"
      if [[ -n "$image_ref" && -n "$expected" ]]; then
        actual="$(image_digest "$image_ref")"
        [[ "$actual" == "$expected" ]] || {
          echo "error: imported digest mismatch: expected=$expected actual=$actual" >&2
          exit 1
        }
      fi
    fi
    ;;
  *) echo "unknown operation: $1" >&2; exit 2 ;;
esac
