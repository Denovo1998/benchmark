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
parser.add_argument("--target-rate", type=float)
parser.add_argument("--max-publish-p50-ms", type=float)
parser.add_argument("--max-publish-p99-ms", type=float)
args = parser.parse_args()

path = pathlib.Path(args.result_json)
data = json.loads(path.read_text())
samples = data.get("samples", [])
if not samples:
    raise SystemExit("result contains no one-second samples")
publish_rates = [float(sample["publishRate"]) for sample in samples]
consume_rates = [float(sample["consumeRate"]) for sample in samples]
backlogs = [int(sample.get("backlogMessages", sample.get("backlog", 0))) for sample in samples]
in_flight = [int(sample.get("inFlightSends", 0)) for sample in samples]
sample_messages_sent = sum(int(sample["messagesSent"]) for sample in samples)
sample_messages_received = sum(int(sample["messagesReceived"]) for sample in samples)
sample_send_errors = sum(int(sample["messageSendErrors"]) for sample in samples)
sample_ack_errors = sum(int(sample.get("ackErrors", 0)) for sample in samples)
measurement_boundary_applied = bool(data.get("measurementDrainApplied", False))
messages_sent = (
    int(data["measurementDrainMessagesSent"])
    if measurement_boundary_applied and "measurementDrainMessagesSent" in data
    else sample_messages_sent
)
messages_received = (
    int(data["measurementDrainMessagesReceived"])
    if measurement_boundary_applied and "measurementDrainMessagesReceived" in data
    else sample_messages_received
)
errors = (
    int(data["measurementDrainMessageSendErrors"])
    if measurement_boundary_applied and "measurementDrainMessageSendErrors" in data
    else sample_send_errors
)
ack_errors = (
    int(data["measurementDrainAckErrors"])
    if measurement_boundary_applied and "measurementDrainAckErrors" in data
    else sample_ack_errors
)
attempted = errors + messages_sent
target_rate = args.target_rate or float(data.get("targetPublishRate", 0))


def sample_duration(sample):
    sent = int(sample.get("messagesSent", 0))
    publish_rate = float(sample.get("publishRate", 0))
    if sent > 0 and publish_rate > 0:
        return sent / publish_rate
    received = int(sample.get("messagesReceived", 0))
    consume_rate = float(sample.get("consumeRate", 0))
    if received > 0 and consume_rate > 0:
        return received / consume_rate
    return 0.0


def parse_time(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


measurement_started_at = parse_time(data.get("measurementStartedAt"))
measurement_ended_at = parse_time(data.get("measurementEndedAt"))
measurement_completed_at = parse_time(data.get("measurementCompletedAt"))
sample_measurement_seconds = sum(sample_duration(sample) for sample in samples)
recorded_measurement_seconds = float(data.get("measurementDurationSeconds", 0))
measurement_seconds = (
    recorded_measurement_seconds
    if recorded_measurement_seconds > 0
    else (
        (measurement_ended_at - measurement_started_at).total_seconds()
        if measurement_started_at is not None
        and measurement_ended_at is not None
        and measurement_ended_at > measurement_started_at
        else sample_measurement_seconds
    )
)
publish_rate_average = messages_sent / measurement_seconds if measurement_seconds else 0.0
consume_rate_average = messages_received / measurement_seconds if measurement_seconds else 0.0
measurement_timestamps_ordered = (
    measurement_started_at is not None
    and measurement_ended_at is not None
    and measurement_completed_at is not None
    and measurement_started_at <= measurement_ended_at <= measurement_completed_at
)


def regression_slope(values):
    if len(values) < 2:
        return 0.0
    points = [
        (float(samples[index].get("elapsedSeconds", index)), float(value))
        for index, value in values
    ]
    mean_x = statistics.mean(x for x, _ in points)
    mean_y = statistics.mean(y for _, y in points)
    denominator = sum((x - mean_x) ** 2 for x, _ in points)
    if denominator == 0:
        return 0.0
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / denominator


half_start = len(samples) // 2
backlog_slope = regression_slope(list(enumerate(backlogs))[half_start:])
in_flight_slope = regression_slope(list(enumerate(in_flight))[half_start:])
output = {
    "runId": data.get("run", {}).get("runId"),
    "repetition": data.get("run", {}).get("repetition"),
    "sampleCount": len(samples),
    "stage": data.get("run", {}).get("stage"),
    "targetPublishRate": target_rate,
    "measurementSeconds": measurement_seconds,
    "publishRateAverage": publish_rate_average,
    "publishRateMedian": statistics.median(publish_rates),
    "publishRateMin": min(publish_rates),
    "publishRateMax": max(publish_rates),
    "consumeRateAverage": consume_rate_average,
    "consumeRateMedian": statistics.median(consume_rates),
    "consumeRateMin": min(consume_rates),
    "consumeRateMax": max(consume_rates),
    "backlogStart": backlogs[0],
    "backlogEnd": backlogs[-1],
    "backlogDelta": backlogs[-1] - backlogs[0],
    "backlogSecondHalfSlopeMessagesPerSecond": backlog_slope,
    "inFlightStart": in_flight[0],
    "inFlightEnd": in_flight[-1],
    "inFlightSecondHalfSlopeMessagesPerSecond": in_flight_slope,
    "messagesSent": messages_sent,
    "messagesReceived": messages_received,
    "messageSendErrors": errors,
    "ackErrors": ack_errors,
    "publishErrorRatio": errors / attempted if attempted else 0.0,
    "publishLatencyMs": {
        "p50": float(data.get("aggregatedPublishLatency50pct", 0)),
        "p95": float(data.get("aggregatedPublishLatency95pct", 0)),
        "p99": float(data.get("aggregatedPublishLatency99pct", 0)),
    },
    "endToEndLatencyMs": {
        "p50": float(data.get("aggregatedEndToEndLatency50pct", 0)),
        "p95": float(data.get("aggregatedEndToEndLatency95pct", 0)),
        "p99": float(data.get("aggregatedEndToEndLatency99pct", 0)),
    },
    "warmupBoundary": {
        "applied": bool(data.get("warmupDrainApplied", False)),
        "durationSeconds": float(data.get("warmupDrainDurationSeconds", 0)),
        "messageSendErrors": int(data.get("warmupDrainMessageSendErrors", 0)),
        "messagesSent": int(data.get("warmupDrainMessagesSent", 0)),
        "messagesReceived": int(data.get("warmupDrainMessagesReceived", 0)),
        "messagesAcknowledged": int(data.get("warmupDrainMessagesAcknowledged", 0)),
        "ackErrors": int(data.get("warmupDrainAckErrors", 0)),
        "acknowledgementTrackingSupported": bool(
            data.get("warmupDrainAcknowledgementTrackingSupported", False)
        ),
        "finalInFlightSends": int(data.get("warmupDrainInFlightSends", 0)),
        "finalAckInFlight": int(data.get("warmupDrainAckInFlight", 0)),
        "finalBacklogMessages": int(data.get("warmupDrainBacklogMessages", 0)),
        "finalBrokerBacklogMessages": data.get("warmupDrainBrokerBacklogMessages"),
        "brokerBacklogZeroPolls": int(data.get("warmupDrainBrokerBacklogZeroPolls", 0)),
        "measurementStartedAt": data.get("measurementStartedAt"),
    },
    "measurementBoundary": {
        "applied": measurement_boundary_applied,
        "durationSeconds": float(data.get("measurementDrainDurationSeconds", 0)),
        "messageSendErrors": int(data.get("measurementDrainMessageSendErrors", 0)),
        "messagesSent": int(data.get("measurementDrainMessagesSent", 0)),
        "messagesReceived": int(data.get("measurementDrainMessagesReceived", 0)),
        "messagesAcknowledged": int(data.get("measurementDrainMessagesAcknowledged", 0)),
        "ackErrors": int(data.get("measurementDrainAckErrors", 0)),
        "acknowledgementTrackingSupported": bool(
            data.get("measurementDrainAcknowledgementTrackingSupported", False)
        ),
        "finalInFlightSends": int(data.get("measurementDrainInFlightSends", 0)),
        "finalAckInFlight": int(data.get("measurementDrainAckInFlight", 0)),
        "finalBacklogMessages": int(data.get("measurementDrainBacklogMessages", 0)),
        "finalBrokerBacklogMessages": data.get("measurementDrainBrokerBacklogMessages"),
        "brokerBacklogZeroPolls": int(
            data.get("measurementDrainBrokerBacklogZeroPolls", 0)
        ),
        "measurementEndedAt": data.get("measurementEndedAt"),
        "measurementCompletedAt": data.get("measurementCompletedAt"),
        "measurementDurationSeconds": recorded_measurement_seconds,
    },
}

if target_rate > 0:
    checks = {
        "cleanWarmupBoundary": (
            bool(data.get("warmupDrainApplied", False))
            and bool(data.get("warmupDrainAcknowledgementTrackingSupported", False))
            and int(data.get("warmupDrainMessageSendErrors", -1)) == 0
            and int(data.get("warmupDrainAckErrors", -1)) == 0
            and int(data.get("warmupDrainInFlightSends", -1)) == 0
            and int(data.get("warmupDrainAckInFlight", -1)) == 0
            and int(data.get("warmupDrainMessagesAcknowledged", -1))
            == int(data.get("warmupDrainMessagesReceived", -2))
            and int(data.get("warmupDrainBacklogMessages", -1)) == 0
            and data.get("warmupDrainBrokerBacklogMessages") == 0
            and int(data.get("warmupDrainBrokerBacklogZeroPolls", 0)) >= 2
            and measurement_started_at is not None
        ),
        "cleanMeasurementBoundary": (
            measurement_boundary_applied
            and bool(data.get("measurementDrainAcknowledgementTrackingSupported", False))
            and int(data.get("measurementDrainMessageSendErrors", -1)) == 0
            and int(data.get("measurementDrainAckErrors", -1)) == 0
            and int(data.get("measurementDrainInFlightSends", -1)) == 0
            and int(data.get("measurementDrainAckInFlight", -1)) == 0
            and int(data.get("measurementDrainMessagesAcknowledged", -1))
            == int(data.get("measurementDrainMessagesReceived", -2))
            and int(data.get("measurementDrainBacklogMessages", -1)) == 0
            and data.get("measurementDrainBrokerBacklogMessages") == 0
            and int(data.get("measurementDrainBrokerBacklogZeroPolls", 0)) >= 2
            and recorded_measurement_seconds > 0
            and measurement_timestamps_ordered
        ),
        "publishRateAtLeast98PercentOfTarget": publish_rate_average >= target_rate * 0.98,
        "consumeRateAtLeast99PercentOfPublish": consume_rate_average >= publish_rate_average * 0.99,
        "publishErrorRatioBelow1e6": (errors / attempted if attempted else 0.0) < 1e-6,
        "noAcknowledgementErrors": ack_errors == 0 and sample_ack_errors == 0,
        "backlogSlopeWithinPoint1PercentOfTarget": backlog_slope <= target_rate * 0.001,
        "endingBacklogWithinFiveSeconds": backlogs[-1] <= target_rate * 5,
        "inFlightNotGrowing": in_flight_slope <= target_rate * 0.001,
    }
    output["sustainability"] = {
        "dataPlanePass": all(checks.values()),
        "checks": checks,
        "externalEvidenceRequired": [
            "OMB worker CPU below the campaign limit",
            "OMB worker network below the campaign limit",
            "broker and bookie CPU, GC, network and disk evidence",
        ],
    }

latency_checks = {}
if args.max_publish_p50_ms is not None:
    latency_checks["publishP50WithinLimit"] = (
        output["publishLatencyMs"]["p50"] <= args.max_publish_p50_ms
    )
if args.max_publish_p99_ms is not None:
    latency_checks["publishP99WithinLimit"] = (
        output["publishLatencyMs"]["p99"] <= args.max_publish_p99_ms
    )
if latency_checks:
    output["latencyGate"] = {
        "pass": all(latency_checks.values()),
        "maxPublishP50Ms": args.max_publish_p50_ms,
        "maxPublishP99Ms": args.max_publish_p99_ms,
        "checks": latency_checks,
    }
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
