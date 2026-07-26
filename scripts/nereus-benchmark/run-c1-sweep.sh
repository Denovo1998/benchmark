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

if [[ $# -lt 5 ]]; then
  echo "usage: $0 STAGE BLOCK_ID REPETITION SEED WORKERS_FILE [RATE ...]" >&2
  exit 2
fi

stage="$1"
block_id="$2"
repetition="$3"
seed="$4"
workers_file="$5"
shift 5
rates=("$@")
if [[ ${#rates[@]} -eq 0 ]]; then
  rates=(50000 75000 100000 150000 200000 300000 400000 600000 800000 1000000)
fi

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
campaign_id="${CAMPAIGN_ID:-v010-202607}"
work_root="${NEREUS_C1_WORK_ROOT:-${repo_root}/results/${campaign_id}/c1-sweep-configs}"
mkdir -p "$work_root"
summary="$work_root/summary.jsonl"
: > "$summary"

for rate in "${rates[@]}"; do
  [[ "$rate" =~ ^[1-9][0-9]*$ ]] || { echo "invalid rate: $rate" >&2; exit 2; }
  run_id="${block_id}-stage-${stage}-rep-${repetition}-rate-${rate}"
  driver_yaml="$work_root/${run_id}-driver.yaml"
  workload_yaml="$work_root/${run_id}-workload.yaml"

  bash "$script_dir/render-run-config.sh" \
    "$stage" "$block_id" "$run_id" "$repetition" "$seed" "$driver_yaml"
  python3 - "$repo_root/workloads/nereus-v0.1.0/c1-throughput-template.yaml" "$workload_yaml" "$rate" "$run_id" <<'PY'
import pathlib
import sys

source, target, rate, run_id = sys.argv[1:]
text = pathlib.Path(source).read_text()
text = text.replace("name: nereus-v010-c1-throughput", f"name: nereus-v010-c1-{run_id}")
text = text.replace("producerRate: 100000", f"producerRate: {rate}")
pathlib.Path(target).write_text(text)
PY

  set +e
  bash "$script_dir/run-case.sh" "$driver_yaml" "$workload_yaml" "$workers_file"
  code=$?
  set -e
  result_dir="$repo_root/results/$campaign_id/$run_id"
  if [[ ! -r "$result_dir/manifest.json" ]]; then
    echo "missing manifest for C1 candidate $run_id" >&2
    exit 1
  fi
  printf '{"runId":"%s","rate":%s,"exitCode":%s,"resultDir":"%s"}\n' \
    "$run_id" "$rate" "$code" "$result_dir" >> "$summary"
done

echo "C1 sweep completed; candidate status: $summary"
