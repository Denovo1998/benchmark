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

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "analyze-run.py"
SELECTOR = pathlib.Path(__file__).parents[1] / "select-common-rate.py"


class AnalyzeRunTest(unittest.TestCase):
    def test_reports_clean_sustainable_window(self):
        result = self.base_result()

        analysis = self.analyze(result, "--max-publish-p50-ms", "4", "--max-publish-p99-ms", "8")

        self.assertTrue(analysis["sustainability"]["dataPlanePass"])
        self.assertTrue(analysis["latencyGate"]["pass"])
        self.assertEqual(analysis["warmupBoundary"]["finalInFlightSends"], 0)
        self.assertEqual(analysis["warmupBoundary"]["finalBacklogMessages"], 0)
        self.assertEqual(analysis["warmupBoundary"]["finalBrokerBacklogMessages"], 0)
        self.assertEqual(analysis["warmupBoundary"]["messageSendErrors"], 0)
        self.assertEqual(analysis["warmupBoundary"]["brokerBacklogZeroPolls"], 2)
        self.assertEqual(analysis["warmupBoundary"]["messagesAcknowledged"], 1)
        self.assertTrue(analysis["warmupBoundary"]["acknowledgementTrackingSupported"])
        self.assertEqual(analysis["measurementBoundary"]["messagesAcknowledged"], 400)
        self.assertEqual(analysis["measurementBoundary"]["finalAckInFlight"], 0)
        self.assertTrue(
            analysis["sustainability"]["checks"]["cleanMeasurementBoundary"]
        )

    def test_rejects_growing_backlog(self):
        result = self.base_result()
        for index, sample in enumerate(result["samples"]):
            sample["backlog"] = index * 100

        analysis = self.analyze(result)

        self.assertFalse(analysis["sustainability"]["dataPlanePass"])
        self.assertFalse(
            analysis["sustainability"]["checks"]["backlogSlopeWithinPoint1PercentOfTarget"]
        )

    def test_rejects_unacknowledged_warmup_backlog(self):
        result = self.base_result()
        result["warmupDrainBrokerBacklogMessages"] = 1

        analysis = self.analyze(result)

        self.assertFalse(analysis["sustainability"]["dataPlanePass"])
        self.assertFalse(analysis["sustainability"]["checks"]["cleanWarmupBoundary"])

    def test_rejects_warmup_send_errors(self):
        result = self.base_result()
        result["warmupDrainMessageSendErrors"] = 1

        analysis = self.analyze(result)

        self.assertFalse(analysis["sustainability"]["dataPlanePass"])
        self.assertFalse(analysis["sustainability"]["checks"]["cleanWarmupBoundary"])

    def test_requires_two_stable_zero_backlog_polls(self):
        result = self.base_result()
        result["warmupDrainBrokerBacklogZeroPolls"] = 1

        analysis = self.analyze(result)

        self.assertFalse(analysis["sustainability"]["dataPlanePass"])
        self.assertFalse(analysis["sustainability"]["checks"]["cleanWarmupBoundary"])

    def test_rejects_unclean_warmup_acknowledgement_boundary(self):
        mutations = {
            "ack error": ("warmupDrainAckErrors", 1),
            "ack still in flight": ("warmupDrainAckInFlight", 1),
            "ack count mismatch": ("warmupDrainMessagesAcknowledged", 0),
            "tracking unavailable": (
                "warmupDrainAcknowledgementTrackingSupported",
                False,
            ),
        }
        for label, (field, value) in mutations.items():
            with self.subTest(label=label):
                result = self.base_result()
                result[field] = value

                analysis = self.analyze(result)

                self.assertFalse(analysis["sustainability"]["dataPlanePass"])
                self.assertFalse(
                    analysis["sustainability"]["checks"]["cleanWarmupBoundary"]
                )

    def test_rejects_unclean_measurement_boundary(self):
        mutations = {
            "boundary missing": ("measurementDrainApplied", False),
            "send still in flight": ("measurementDrainInFlightSends", 1),
            "ack error": ("measurementDrainAckErrors", 1),
            "ack still in flight": ("measurementDrainAckInFlight", 1),
            "ack count mismatch": ("measurementDrainMessagesAcknowledged", 399),
            "broker backlog": ("measurementDrainBrokerBacklogMessages", 1),
            "tracking unavailable": (
                "measurementDrainAcknowledgementTrackingSupported",
                False,
            ),
            "completion timestamp missing": ("measurementCompletedAt", None),
        }
        for label, (field, value) in mutations.items():
            with self.subTest(label=label):
                result = self.base_result()
                result[field] = value

                analysis = self.analyze(result)

                self.assertFalse(analysis["sustainability"]["dataPlanePass"])
                self.assertFalse(
                    analysis["sustainability"]["checks"]["cleanMeasurementBoundary"]
                )

    def analyze(self, result, *args):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "result.json"
            path.write_text(json.dumps(result))
            completed = subprocess.run(
                [sys.executable, str(SCRIPT), str(path), *args],
                check=True,
                capture_output=True,
                text=True,
            )
        return json.loads(completed.stdout)

    @staticmethod
    def base_result():
        samples = []
        for second in range(1, 5):
            samples.append(
                {
                    "elapsedSeconds": second,
                    "messagesSent": 100,
                    "messagesReceived": 100,
                    "messageSendErrors": 0,
                    "inFlightSends": 1,
                    "messagesAcknowledged": 100,
                    "ackErrors": 0,
                    "ackInFlight": 1,
                    "backlog": 0,
                    "publishRate": 100.0,
                    "consumeRate": 100.0,
                }
            )
        return {
            "run": {"stage": "A", "runId": "A-100", "repetition": 1},
            "targetPublishRate": 100.0,
            "measurementStartedAt": "2026-08-08T00:00:00Z",
            "warmupDrainApplied": True,
            "warmupDrainDurationSeconds": 1.0,
            "warmupDrainMessageSendErrors": 0,
            "warmupDrainMessagesSent": 1,
            "warmupDrainMessagesReceived": 1,
            "warmupDrainMessagesAcknowledged": 1,
            "warmupDrainAckErrors": 0,
            "warmupDrainAckInFlight": 0,
            "warmupDrainAcknowledgementTrackingSupported": True,
            "warmupDrainInFlightSends": 0,
            "warmupDrainBacklogMessages": 0,
            "warmupDrainBrokerBacklogMessages": 0,
            "warmupDrainBrokerBacklogZeroPolls": 2,
            "measurementEndedAt": "2026-08-08T00:00:04Z",
            "measurementCompletedAt": "2026-08-08T00:00:05Z",
            "measurementDurationSeconds": 4.0,
            "measurementDrainApplied": True,
            "measurementDrainDurationSeconds": 1.0,
            "measurementDrainMessagesSent": 400,
            "measurementDrainMessagesReceived": 400,
            "measurementDrainMessageSendErrors": 0,
            "measurementDrainInFlightSends": 0,
            "measurementDrainMessagesAcknowledged": 400,
            "measurementDrainAckErrors": 0,
            "measurementDrainAckInFlight": 0,
            "measurementDrainAcknowledgementTrackingSupported": True,
            "measurementDrainBacklogMessages": 0,
            "measurementDrainBrokerBacklogMessages": 0,
            "measurementDrainBrokerBacklogZeroPolls": 2,
            "aggregatedPublishLatency50pct": 3.0,
            "aggregatedPublishLatency95pct": 5.0,
            "aggregatedPublishLatency99pct": 7.0,
            "aggregatedEndToEndLatency50pct": 10.0,
            "aggregatedEndToEndLatency95pct": 15.0,
            "aggregatedEndToEndLatency99pct": 20.0,
            "samples": samples,
        }


class SelectCommonRateTest(unittest.TestCase):
    def test_selects_highest_rate_passed_by_both_stages(self):
        analyses = [
            self.analysis("A", 10_000, True),
            self.analysis("B", 10_000, True),
            self.analysis("A", 20_000, True),
            self.analysis("B", 20_000, False),
        ]
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index, analysis in enumerate(analyses):
                path = pathlib.Path(directory) / f"analysis-{index}.json"
                path.write_text(json.dumps(analysis))
                paths.append(str(path))
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SELECTOR),
                    *paths,
                    "--min-repetitions",
                    "1",
                ],
                check=True,
                capture_output=True,
                text=True,
            )

        selection = json.loads(completed.stdout)
        self.assertEqual(selection["selectedCommonRate"], 10_000)

    def test_does_not_count_duplicate_repetition_as_independent_runs(self):
        analyses = [
            self.analysis("A", 10_000, True),
            self.analysis("A", 10_000, True),
            self.analysis("B", 10_000, True),
            self.analysis("B", 10_000, True),
        ]
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index, analysis in enumerate(analyses):
                path = pathlib.Path(directory) / f"analysis-{index}.json"
                path.write_text(json.dumps(analysis))
                paths.append(str(path))
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SELECTOR),
                    *paths,
                    "--min-repetitions",
                    "2",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(completed.returncode, 1)
        selection = json.loads(completed.stdout)
        self.assertIsNone(selection["selectedCommonRate"])
        self.assertEqual(selection["candidates"][0]["stages"]["A"]["distinctRepetitions"], 1)

    @staticmethod
    def analysis(stage, rate, passed):
        return {
            "stage": stage,
            "runId": f"{stage}-{rate}",
            "repetition": 1,
            "targetPublishRate": rate,
            "sustainability": {"dataPlanePass": passed},
            "latencyGate": {"pass": passed},
        }


if __name__ == "__main__":
    unittest.main()
