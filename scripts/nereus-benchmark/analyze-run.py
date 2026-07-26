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

"""Analyze OMB samples without changing the raw result or manifest.

The optional fault-events file lets R1 use the one-second sample timestamps to
calculate the first 30-second window that returns to 95% of the pre-fault
two-minute median while backlog is not growing.
"""
import argparse
import json
import pathlib
import statistics
import sys
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument("result_json")
parser.add_argument("--fault-events")
args = parser.parse_args()

path = pathlib.Path(args.result_json)
data = json.loads(path.read_text())
samples = data.get("samples", [])
if not samples:
    raise SystemExit("result contains no one-second samples")
rates = [float(sample["consumeRate"]) for sample in samples]
backlogs = [int(sample.get("backlogMessages", sample.get("backlog", 0))) for sample in samples]
errors = sum(int(sample["messageSendErrors"]) for sample in samples)
attempted = errors + sum(int(sample["messagesSent"]) for sample in samples)
output = {
    "sampleCount": len(samples),
    "consumeRateMedian": statistics.median(rates),
    "consumeRateMin": min(rates),
    "consumeRateMax": max(rates),
    "backlogStart": backlogs[0],
    "backlogEnd": backlogs[-1],
    "backlogDelta": backlogs[-1] - backlogs[0],
    "messageSendErrors": errors,
    "publishErrorRatio": errors / attempted if attempted else 0.0,
}


def parse_time(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


if args.fault_events:
    events = []
    for line in pathlib.Path(args.fault_events).read_text().splitlines():
        if line.strip():
            event = json.loads(line)
            timestamp = parse_time(event.get("requestTime", event.get("timestamp")))
            if timestamp is not None:
                events.append((timestamp, event))
    if events:
        fault_time, event = events[-1]
        timed_samples = [(parse_time(sample.get("timestamp")), sample) for sample in samples]
        timed_samples = [(timestamp, sample) for timestamp, sample in timed_samples if timestamp]
        before = [
            sample
            for timestamp, sample in timed_samples
            if -120 <= (timestamp - fault_time).total_seconds() < 0
        ]
        if before:
            publish_baseline = statistics.median(float(sample["publishRate"]) for sample in before)
            consume_baseline = statistics.median(float(sample["consumeRate"]) for sample in before)
            threshold = min(publish_baseline, consume_baseline) * 0.95
            after = [(timestamp, sample) for timestamp, sample in timed_samples if timestamp >= fault_time]
            recovery = None
            for index in range(len(after)):
                window = after[index : index + 30]
                if len(window) < 30:
                    break
                if all(
                    float(sample["publishRate"]) >= threshold
                    and float(sample["consumeRate"]) >= threshold
                    for _, sample in window
                ) and int(window[-1][1].get("backlogMessages", window[-1][1].get("backlog", 0))) <= int(
                    window[0][1].get("backlogMessages", window[0][1].get("backlog", 0))
                ):
                    recovery = max(0.0, (window[0][0] - fault_time).total_seconds())
                    break
            output["r1"] = {
                "faultRequestTime": fault_time.isoformat(),
                "preFaultPublishMedian": publish_baseline,
                "preFaultConsumeMedian": consume_baseline,
                "recoveryThreshold": threshold,
                "recoverySeconds": recovery,
                "replacementReadyAt": event.get("replacementReadyAt"),
            }

print(json.dumps(output, indent=2, sort_keys=True))
