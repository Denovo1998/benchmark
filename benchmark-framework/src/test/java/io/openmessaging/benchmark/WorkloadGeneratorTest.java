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
package io.openmessaging.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.RunConfiguration;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class WorkloadGeneratorTest {

    @Test
    void drainsWarmupBeforeResettingMeasurementStats() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats pending = counters(10, 9, 1);
        CountersStats drained = counters(10, 10, 0);
        when(worker.getCountersStats()).thenReturn(pending, drained, drained);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(3), worker);
        generator.prepareMeasurementWindow();

        InOrder order = inOrder(worker);
        order.verify(worker).pauseProducers();
        order.verify(worker, times(3)).getCountersStats();
        order.verify(worker).resetStats();
        order.verify(worker).resumeProducers();
        assertThat(generator.getMeasurementStartedAt()).isNotBlank();
        RunManifest manifest = new RunManifest();
        generator.copyBoundaryEvidenceTo(manifest);
        assertThat(manifest.warmupDrainMessageSendErrors).isZero();
        assertThat(manifest.warmupDrainBrokerBacklogZeroPolls).isEqualTo(2);
        assertThat(manifest.warmupDrainMessagesAcknowledged).isEqualTo(10);
        assertThat(manifest.warmupDrainAcknowledgementTrackingSupported).isTrue();
    }

    @Test
    void waitsForBrokerAcknowledgementsBeforeResettingMeasurementStats() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats drained = counters(10, 10, 0);
        when(worker.getCountersStats()).thenReturn(drained);
        when(worker.getSubscriptionBacklog("topic", "subscription")).thenReturn(1L, 0L, 0L);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(4), worker);
        addBacklogTarget(generator, new TopicSubscription("topic", "subscription"));
        generator.prepareMeasurementWindow();

        InOrder order = inOrder(worker);
        order.verify(worker).pauseProducers();
        order.verify(worker).getCountersStats();
        order.verify(worker).getSubscriptionBacklog("topic", "subscription");
        order.verify(worker).getCountersStats();
        order.verify(worker).getSubscriptionBacklog("topic", "subscription");
        order.verify(worker).getCountersStats();
        order.verify(worker).getSubscriptionBacklog("topic", "subscription");
        order.verify(worker).resetStats();
        order.verify(worker).resumeProducers();
    }

    @Test
    void keepsProducersPausedWhenDrainInspectionFails() throws Exception {
        Worker worker = mock(Worker.class);
        when(worker.getCountersStats()).thenThrow(new IOException("stats unavailable"));

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(1), worker);

        assertThatExceptionOfType(InvalidBenchmarkRunException.class)
                .isThrownBy(generator::prepareMeasurementWindow)
                .withMessageContaining("clean pre-measurement boundary")
                .withCauseInstanceOf(IOException.class);
        verify(worker).pauseProducers();
        verify(worker, never()).resumeProducers();
        verify(worker, never()).resetStats();
    }

    @Test
    void invalidatesWarmupWithSendErrorsWithoutResettingOrResuming() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats failed = counters(10, 10, 1, 0);
        when(worker.getCountersStats()).thenReturn(failed);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(3), worker);

        assertThatExceptionOfType(InvalidBenchmarkRunException.class)
                .isThrownBy(generator::prepareMeasurementWindow)
                .withMessageContaining("sendErrors=1");
        verify(worker).pauseProducers();
        verify(worker, never()).resetStats();
        verify(worker, never()).resumeProducers();
        RunManifest manifest = new RunManifest();
        generator.copyBoundaryEvidenceTo(manifest);
        assertThat(manifest.warmupDrainMessageSendErrors).isEqualTo(1);
    }

    @Test
    void waitsForAcknowledgementCompletionBeforeResettingMeasurementStats() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats pendingAck = ackCounters(10, 10, 9, 0, 1);
        CountersStats drained = ackCounters(10, 10, 10, 0, 0);
        when(worker.getCountersStats()).thenReturn(pendingAck, drained, drained);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(3), worker);
        generator.prepareMeasurementWindow();

        verify(worker, times(3)).getCountersStats();
        verify(worker).resetStats();
        verify(worker).resumeProducers();
    }

    @Test
    void invalidatesWarmupWhenAcknowledgementFails() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats failed = ackCounters(10, 10, 9, 1, 0);
        when(worker.getCountersStats()).thenReturn(failed);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(3), worker);

        assertThatExceptionOfType(InvalidBenchmarkRunException.class)
                .isThrownBy(generator::prepareMeasurementWindow)
                .withMessageContaining("ackErrors=1");
        RunManifest manifest = new RunManifest();
        generator.copyBoundaryEvidenceTo(manifest);
        assertThat(manifest.warmupDrainAckErrors).isEqualTo(1);
        assertThat(manifest.warmupDrainMessagesAcknowledged).isEqualTo(9);
        verify(worker, never()).resetStats();
        verify(worker, never()).resumeProducers();
    }

    @Test
    void invalidatesWarmupWhenDeliveryCountShowsRedelivery() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats redelivered = ackCounters(10, 11, 11, 0, 0);
        when(worker.getCountersStats()).thenReturn(redelivered);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(3), worker);

        assertThatExceptionOfType(InvalidBenchmarkRunException.class)
                .isThrownBy(generator::prepareMeasurementWindow)
                .withMessageContaining("possible redelivery");
        RunManifest manifest = new RunManifest();
        generator.copyBoundaryEvidenceTo(manifest);
        assertThat(manifest.warmupDrainMessagesReceived).isEqualTo(11);
        assertThat(manifest.warmupDrainBacklogMessages).isZero();
    }

    @Test
    void drainsMeasurementAcknowledgementsBeforeCompletingWindow() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats warmupDrained = ackCounters(1, 1, 1, 0, 0);
        CountersStats pendingAck = ackCounters(20, 20, 19, 0, 1);
        CountersStats drained = ackCounters(20, 20, 20, 0, 0);
        when(worker.getCountersStats())
                .thenReturn(warmupDrained, warmupDrained, pendingAck, drained, drained);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(3), worker);
        generator.prepareMeasurementWindow();
        generator.completeMeasurementWindow();

        verify(worker, times(2)).pauseProducers();
        verify(worker, times(5)).getCountersStats();
        RunManifest manifest = new RunManifest();
        generator.copyBoundaryEvidenceTo(manifest);
        assertThat(manifest.measurementDrainApplied).isTrue();
        assertThat(manifest.measurementDurationSeconds).isPositive();
        assertThat(manifest.measurementEndedAt).isNotBlank();
        assertThat(manifest.measurementCompletedAt).isNotBlank();
        assertThat(manifest.measurementDrainMessagesAcknowledged).isEqualTo(20);
        assertThat(manifest.measurementDrainAckInFlight).isZero();
        assertThat(manifest.measurementDrainBrokerBacklogZeroPolls).isEqualTo(2);
    }

    @Test
    void keepsMeasurementWindowInvalidWhenFinalAcknowledgementFails() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats failed = ackCounters(20, 20, 19, 1, 0);
        when(worker.getCountersStats()).thenReturn(failed);

        WorkloadGenerator generator = new WorkloadGenerator("driver", workload(3), worker);

        assertThatExceptionOfType(InvalidBenchmarkRunException.class)
                .isThrownBy(generator::completeMeasurementWindow)
                .withMessageContaining("ackErrors=1");
        RunManifest manifest = new RunManifest();
        generator.copyBoundaryEvidenceTo(manifest);
        assertThat(manifest.measurementEndedAt).isNotBlank();
        assertThat(manifest.measurementCompletedAt).isNull();
        assertThat(manifest.measurementDrainAckErrors).isEqualTo(1);
    }

    @Test
    void formalRunRequiresAcknowledgementTracking() throws Exception {
        Worker worker = mock(Worker.class);
        CountersStats unsupported = counters(1, 1, 0);
        unsupported.acknowledgementTrackingSupported = false;
        when(worker.getCountersStats()).thenReturn(unsupported);
        WorkloadGenerator generator = new WorkloadGenerator("driver", formalRun(), workload(3), worker);

        assertThatExceptionOfType(InvalidBenchmarkRunException.class)
                .isThrownBy(generator::prepareMeasurementWindow)
                .withMessageContaining("does not expose acknowledgement completion");
        RunManifest manifest = new RunManifest();
        generator.copyBoundaryEvidenceTo(manifest);
        assertThat(manifest.warmupDrainAcknowledgementTrackingSupported).isFalse();
    }

    @Test
    void formalWarmupRequiresCleanBoundaryTimeout() {
        Workload workload = workload(0);
        RunConfiguration run = formalRun();

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new WorkloadGenerator("driver", run, workload, mock(Worker.class)))
                .withMessageContaining("warmupDrainTimeoutSeconds")
                .withMessageContaining("measurementDrainTimeoutSeconds");
    }

    private static Workload workload(int drainTimeoutSeconds) {
        Workload workload = new Workload();
        workload.name = "test";
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 1;
        workload.subscriptionsPerTopic = 1;
        workload.producersPerTopic = 1;
        workload.consumerPerSubscription = 1;
        workload.producerRate = 1;
        workload.warmupDurationMinutes = 1;
        workload.warmupDrainTimeoutSeconds = drainTimeoutSeconds;
        workload.measurementDrainTimeoutSeconds = drainTimeoutSeconds;
        workload.testDurationMinutes = 1;
        return workload;
    }

    private static CountersStats counters(long sent, long received, long inFlight) {
        return counters(sent, received, 0, inFlight);
    }

    private static CountersStats counters(long sent, long received, long sendErrors, long inFlight) {
        CountersStats stats = new CountersStats();
        stats.messagesSent = sent;
        stats.messagesReceived = received;
        stats.messagesAcknowledged = received;
        stats.messageSendErrors = sendErrors;
        stats.inFlightSends = inFlight;
        stats.acknowledgementTrackingSupported = true;
        return stats;
    }

    private static CountersStats ackCounters(
            long sent, long received, long acknowledged, long ackErrors, long ackInFlight) {
        CountersStats stats = counters(sent, received, 0);
        stats.messagesAcknowledged = acknowledged;
        stats.ackErrors = ackErrors;
        stats.ackInFlight = ackInFlight;
        return stats;
    }

    private static RunConfiguration formalRun() {
        RunConfiguration run = new RunConfiguration();
        run.campaignId = "campaign";
        run.blockId = "block";
        run.runId = "run";
        run.stage = "A";
        run.repetition = 1;
        run.seed = 1L;
        return run;
    }

    @SuppressWarnings("unchecked")
    private static void addBacklogTarget(WorkloadGenerator generator, TopicSubscription target)
            throws ReflectiveOperationException {
        Field field = WorkloadGenerator.class.getDeclaredField("backlogTargets");
        field.setAccessible(true);
        ((List<TopicSubscription>) field.get(generator)).add(target);
    }
}
