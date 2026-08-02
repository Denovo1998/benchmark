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

#!/usr/bin/env bash
set -euo pipefail

image="${1:-openmessaging/openmessaging-benchmark:pulsar}"
dockerfile="${DOCKERFILE:-docker/Dockerfile.build}"
context="${CONTEXT:-.}"
proxy_build_args=()

append_proxy_build_arg() {
  local variable_name="$1"
  local variable_value="${!variable_name-}"
  if [[ -n "${variable_value}" ]]; then
    proxy_build_args+=(--build-arg "${variable_name}=${variable_value}")
  fi
}

for proxy_variable in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
  append_proxy_build_arg "${proxy_variable}"
done

if command -v nerdctl >/dev/null 2>&1; then
  # Default to the Kubernetes (CRI) namespace so images are visible to K8s.
  # Set NERDCTL_NAMESPACE explicitly to override (e.g. "default"), or set it to
  # an empty string to omit the namespace flag.
  namespace="${NERDCTL_NAMESPACE-k8s.io}"
  ns_args=()
  if [[ -n "${namespace}" ]]; then
    ns_args=(-n "${namespace}")
  fi
  exec nerdctl "${ns_args[@]}" build \
    "${proxy_build_args[@]}" \
    -t "${image}" \
    -f "${dockerfile}" \
    "${context}"
fi

if command -v buildctl >/dev/null 2>&1; then
  # BuildKit path (no nerdctl). Requires buildkitd running.
  # - Start buildkitd (root):   sudo buildkitd --addr unix:///run/buildkit/buildkitd.sock
  # - Start buildkitd (rootless): buildkitd --addr unix:///run/user/$UID/buildkit/buildkitd.sock
  addr="${BUILDKIT_ADDR:-unix:///run/buildkit/buildkitd.sock}"
  out_dir="${OUT_DIR:-docker/out}"
  mkdir -p "${out_dir}"
  oci_tar="${OCI_TAR:-${out_dir}/$(echo "${image}" | tr '/:' '__').oci.tar}"
  proxy_buildkit_opts=()
  for proxy_variable in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
    proxy_value="${!proxy_variable-}"
    if [[ -n "${proxy_value}" ]]; then
      proxy_buildkit_opts+=(--opt "build-arg:${proxy_variable}=${proxy_value}")
    fi
  done

  buildctl --addr "${addr}" build \
    --frontend dockerfile.v0 \
    --local context="${context}" \
    --local dockerfile="${context}" \
    --opt filename="${dockerfile}" \
    "${proxy_buildkit_opts[@]}" \
    --output "type=oci,dest=${oci_tar}"

  echo "built OCI archive: ${oci_tar}"

  # Optional import into containerd (needs access to containerd socket; often requires sudo).
  if [[ "${IMPORT_TO_CONTAINERD:-0}" == "1" ]]; then
    ns="${CONTAINERD_NAMESPACE:-k8s.io}"
    echo "importing into containerd namespace: ${ns}"
    ctr -n "${ns}" images import "${oci_tar}"
  fi

  exit 0
fi

echo "error: neither nerdctl nor buildctl found." >&2
echo "install nerdctl (recommended) OR install BuildKit (buildctl/buildkitd)." >&2
exit 1
