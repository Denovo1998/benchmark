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

if [[ $# -ne 6 ]]; then
  echo "usage: $0 STAGE BLOCK_ID RUN_ID REPETITION SEED OUTPUT" >&2
  exit 2
fi

stage="$1"
block_id="$2"
run_id="$3"
repetition="$4"
seed="$5"
output="$6"
campaign_id="${CAMPAIGN_ID:-v010-202607}"
template="${TEMPLATE:-driver-pulsar/nereus-v0.1.0/pulsar-stage-template.yaml}"

case "$stage" in
  A|B) storage_class=bookkeeper ;;
  C|D|E) storage_class=nereus ;;
  *) echo "invalid stage: $stage (expected A-E)" >&2; exit 2 ;;
esac

python3 - "$template" "$output" "$stage" "$block_id" "$run_id" "$repetition" "$seed" "$campaign_id" "$storage_class" <<'PY'
import os
import pathlib
import sys

template, output, stage, block_id, run_id, repetition, seed, campaign_id, storage = sys.argv[1:]
text = pathlib.Path(template).read_text()
values = {
    "${STAGE}": stage,
    "${BLOCK_ID}": block_id,
    "${RUN_ID}": run_id,
    "${REPETITION}": repetition,
    "${SEED}": seed,
    "${CAMPAIGN_ID}": campaign_id,
    "${STORAGE_CLASS}": storage,
    "${PULSAR_SERVICE_URL:-pulsar://nereus-broker.pulsar.svc.cluster.local:6650}":
        os.environ.get("PULSAR_SERVICE_URL", "pulsar://nereus-broker.pulsar.svc.cluster.local:6650"),
    "${PULSAR_HTTP_URL:-http://nereus-broker.pulsar.svc.cluster.local:8080}":
        os.environ.get("PULSAR_HTTP_URL", "http://nereus-broker.pulsar.svc.cluster.local:8080"),
    "${PULSAR_CLUSTER:-beijing-1-benchmark}":
        os.environ.get("PULSAR_CLUSTER", "beijing-1-benchmark"),
}
for key, value in values.items():
    text = text.replace(key, value)
pathlib.Path(output).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(output).write_text(text)
PY

echo "rendered $output (stage=$stage storageClass=$storage_class runId=$run_id)"
