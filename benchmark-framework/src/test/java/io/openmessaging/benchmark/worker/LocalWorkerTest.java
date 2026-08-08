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
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.Test;

class LocalWorkerTest {

    @Test
    void waitsForAndTracksSuccessfulReadinessProbe() throws Exception {
        LocalWorker worker = new LocalWorker();
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        when(producer.sendAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        addProducer(worker, producer);

        try {
            worker.probeProducers();

            assertThat(worker.getCountersStats().messagesSent).isEqualTo(1);
            assertThat(worker.getCountersStats().messageSendErrors).isZero();
            assertThat(worker.getCountersStats().inFlightSends).isZero();
        } finally {
            worker.stopAll();
            worker.close();
        }
    }

    @Test
    void failsAndTracksReadinessProbeError() throws Exception {
        LocalWorker worker = new LocalWorker();
        BenchmarkProducer producer = mock(BenchmarkProducer.class);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("probe failed"));
        when(producer.sendAsync(any(), any())).thenReturn(failed);
        addProducer(worker, producer);

        try {
            assertThatIOException()
                    .isThrownBy(worker::probeProducers)
                    .withMessageContaining("readiness probe send failed")
                    .withCauseInstanceOf(IllegalStateException.class);
            assertThat(worker.getCountersStats().messagesSent).isZero();
            assertThat(worker.getCountersStats().messageSendErrors).isEqualTo(1);
            assertThat(worker.getCountersStats().inFlightSends).isZero();
        } finally {
            worker.stopAll();
            worker.close();
        }
    }

    @Test
    void tracksAcknowledgementLifecycle() throws Exception {
        LocalWorker worker = new LocalWorker();
        try {
            worker.internalMessageReceived(10, System.currentTimeMillis());
            worker.messageAcknowledgementStarted();

            assertThat(worker.getCountersStats().messagesReceived).isEqualTo(1);
            assertThat(worker.getCountersStats().messagesAcknowledged).isZero();
            assertThat(worker.getCountersStats().ackInFlight).isEqualTo(1);

            worker.messageAcknowledgementCompleted(null);

            assertThat(worker.getCountersStats().messagesAcknowledged).isEqualTo(1);
            assertThat(worker.getCountersStats().ackErrors).isZero();
            assertThat(worker.getCountersStats().ackInFlight).isZero();
        } finally {
            worker.stopAll();
            worker.close();
        }
    }

    @Test
    void cancelledLoadDoesNotStartAnotherSend() throws Exception {
        WorkerStats stats = new WorkerStats(NullStatsLogger.INSTANCE);
        MessageProducer sender = new MessageProducer(new UniformRateLimiter(1), stats);
        BenchmarkProducer producer = mock(BenchmarkProducer.class);

        sender.sendMessage(producer, Optional.empty(), new byte[1], () -> true);

        verifyNoInteractions(producer);
        assertThat(stats.toCountersStats().messagesSent).isZero();
        assertThat(stats.toCountersStats().inFlightSends).isZero();
    }

    @SuppressWarnings("unchecked")
    private static void addProducer(LocalWorker worker, BenchmarkProducer producer)
            throws ReflectiveOperationException {
        Field field = LocalWorker.class.getDeclaredField("producers");
        field.setAccessible(true);
        ((List<BenchmarkProducer>) field.get(worker)).add(producer);
    }
}
