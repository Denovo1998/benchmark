/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.Test;

class WorkerStatsTest {

    @Test
    void tracksReadinessProbeCompletionInBoundaryCounters() throws Exception {
        WorkerStats stats = new WorkerStats(NullStatsLogger.INSTANCE);

        stats.recordProducerStarted();
        stats.recordProbeSuccess();

        assertThat(stats.toCountersStats().messagesSent).isEqualTo(1);
        assertThat(stats.toCountersStats().messageSendErrors).isZero();
        assertThat(stats.toCountersStats().inFlightSends).isZero();
    }

    @Test
    void resetMeasurementClearsWarmupCountersAndLatencies() throws Exception {
        WorkerStats stats = new WorkerStats(NullStatsLogger.INSTANCE);
        stats.recordProducerStarted();
        stats.recordProducerSuccess(10, 0, 0, 1_000);
        stats.recordMessageReceived(10, 1_000);
        stats.recordAcknowledgementStarted();
        stats.recordAcknowledgementCompleted(null);

        assertThat(stats.toCountersStats().messagesSent).isEqualTo(1);
        assertThat(stats.toCountersStats().messagesReceived).isEqualTo(1);
        assertThat(stats.toCountersStats().messagesAcknowledged).isEqualTo(1);
        CumulativeLatencies beforeReset = stats.toCumulativeLatencies();
        assertThat(beforeReset.publishLatency.getTotalCount()).isEqualTo(1);
        assertThat(beforeReset.endToEndLatency.getTotalCount()).isEqualTo(1);

        stats.resetMeasurement();

        assertThat(stats.toCountersStats().messagesSent).isZero();
        assertThat(stats.toCountersStats().messagesReceived).isZero();
        assertThat(stats.toCountersStats().messageSendErrors).isZero();
        assertThat(stats.toCountersStats().inFlightSends).isZero();
        assertThat(stats.toCountersStats().messagesAcknowledged).isZero();
        assertThat(stats.toCountersStats().ackErrors).isZero();
        assertThat(stats.toCountersStats().ackInFlight).isZero();
        assertThat(stats.toCountersStats().acknowledgementTrackingSupported).isTrue();
        assertThat(stats.toPeriodStats().messagesSent).isZero();
        assertThat(stats.toPeriodStats().messagesReceived).isZero();
        CumulativeLatencies afterReset = stats.toCumulativeLatencies();
        assertThat(afterReset.publishLatency.getTotalCount()).isZero();
        assertThat(afterReset.endToEndLatency.getTotalCount()).isZero();
    }

    @Test
    void exposesAcknowledgementInFlightUntilSuccessfulCompletion() throws Exception {
        WorkerStats stats = new WorkerStats(NullStatsLogger.INSTANCE);
        stats.recordMessageReceived(10, 0);

        stats.recordAcknowledgementStarted();

        CountersStats pending = stats.toCountersStats();
        assertThat(pending.acknowledgementTrackingSupported).isTrue();
        assertThat(pending.messagesReceived).isEqualTo(1);
        assertThat(pending.messagesAcknowledged).isZero();
        assertThat(pending.ackInFlight).isEqualTo(1);

        stats.recordAcknowledgementCompleted(null);

        CountersStats completed = stats.toCountersStats();
        assertThat(completed.messagesAcknowledged).isEqualTo(1);
        assertThat(completed.ackErrors).isZero();
        assertThat(completed.ackInFlight).isZero();
    }

    @Test
    void recordsAcknowledgementAndProducerFailuresBeforeClearingInFlight() throws Exception {
        WorkerStats stats = new WorkerStats(NullStatsLogger.INSTANCE);
        IllegalStateException failure = new IllegalStateException("failed");

        stats.recordProducerStarted();
        stats.recordProducerFailure();
        stats.recordAcknowledgementStarted();
        stats.recordAcknowledgementCompleted(failure);

        CountersStats counters = stats.toCountersStats();
        assertThat(counters.messageSendErrors).isEqualTo(1);
        assertThat(counters.inFlightSends).isZero();
        assertThat(counters.ackErrors).isEqualTo(1);
        assertThat(counters.ackInFlight).isZero();
    }
}
