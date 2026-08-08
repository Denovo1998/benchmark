#!/usr/bin/env python3
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

"""Select the highest rate sustained by every requested benchmark stage."""

import argparse
import collections
import json
import pathlib
import sys


parser = argparse.ArgumentParser()
parser.add_argument("analysis_json", nargs="+")
parser.add_argument("--stages", default="A,B")
parser.add_argument("--min-repetitions", type=int, default=3)
parser.add_argument("--require-latency-gate", action="store_true")
args = parser.parse_args()

stages = [stage.strip() for stage in args.stages.split(",") if stage.strip()]
if not stages:
    raise SystemExit("--stages must contain at least one stage")
if args.min_repetitions <= 0:
    raise SystemExit("--min-repetitions must be positive")

grouped = collections.defaultdict(lambda: collections.defaultdict(list))
for filename in args.analysis_json:
    path = pathlib.Path(filename)
    analysis = json.loads(path.read_text())
    stage = analysis.get("stage")
    rate = float(analysis.get("targetPublishRate", 0))
    if stage not in stages or rate <= 0:
        continue
    analysis["_source"] = str(path)
    grouped[rate][stage].append(analysis)

candidates = []
for rate in sorted(grouped):
    stage_results = {}
    eligible = True
    for stage in stages:
        runs = grouped[rate].get(stage, [])
        repetitions = {
            run.get("repetition") for run in runs if run.get("repetition") is not None
        }
        data_plane_passes = sum(
            bool(run.get("sustainability", {}).get("dataPlanePass")) for run in runs
        )
        latency_gate_passes = sum(bool(run.get("latencyGate", {}).get("pass")) for run in runs)
        stage_eligible = (
            len(repetitions) >= args.min_repetitions and data_plane_passes == len(runs)
        )
        if args.require_latency_gate:
            stage_eligible = stage_eligible and latency_gate_passes == len(runs)
        eligible = eligible and stage_eligible
        stage_results[stage] = {
            "runs": len(runs),
            "distinctRepetitions": len(repetitions),
            "dataPlanePasses": data_plane_passes,
            "latencyGatePasses": latency_gate_passes,
            "eligible": stage_eligible,
            "runIds": [run.get("runId") for run in runs],
            "sources": [run["_source"] for run in runs],
        }
    candidates.append({"rate": rate, "eligible": eligible, "stages": stage_results})

eligible_rates = [candidate["rate"] for candidate in candidates if candidate["eligible"]]
output = {
    "stages": stages,
    "minimumRepetitions": args.min_repetitions,
    "latencyGateRequired": args.require_latency_gate,
    "selectedCommonRate": max(eligible_rates) if eligible_rates else None,
    "externalEvidenceStillRequired": True,
    "candidates": candidates,
}
print(json.dumps(output, indent=2, sort_keys=True))
if not eligible_rates:
    sys.exit(1)
