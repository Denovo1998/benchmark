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

import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

public class WorkerStats {

    private final StatsLogger statsLogger;

    private final OpStatsLogger publishDelayLatencyStats;

    private final Recorder endToEndLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(12), 5);
    private final Recorder endToEndCumulativeLatencyRecorder =
            new Recorder(TimeUnit.HOURS.toMicros(12), 5);
    private final OpStatsLogger endToEndLatencyStats;

    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder messageSendErrors = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final Counter messageSendErrorCounter;
    private final Counter messagesSentCounter;
    private final Counter bytesSentCounter;

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final LongAdder messagesAcknowledged = new LongAdder();
    private final LongAdder ackErrors = new LongAdder();
    private final Counter messagesReceivedCounter;
    private final Counter bytesReceivedCounter;
    private final Counter messagesAcknowledgedCounter;
    private final Counter ackErrorCounter;

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalMessageSendErrors = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();
    private final LongAdder totalMessagesAcknowledged = new LongAdder();
    private final LongAdder totalAckErrors = new LongAdder();
    private final LongAdder inFlightSends = new LongAdder();
    private final LongAdder ackInFlight = new LongAdder();
    private volatile boolean acknowledgementTrackingSupported;

    private static final long highestTrackableValue = TimeUnit.SECONDS.toMicros(60);
    private final Recorder publishLatencyRecorder = new Recorder(highestTrackableValue, 5);
    private final Recorder cumulativePublishLatencyRecorder = new Recorder(highestTrackableValue, 5);
    private final OpStatsLogger publishLatencyStats;

    private final Recorder publishDelayLatencyRecorder = new Recorder(highestTrackableValue, 5);
    private final Recorder cumulativePublishDelayLatencyRecorder =
            new Recorder(highestTrackableValue, 5);

    WorkerStats(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        StatsLogger producerStatsLogger = statsLogger.scope("producer");
        this.messagesSentCounter = producerStatsLogger.getCounter("messages_sent");
        this.messageSendErrorCounter = producerStatsLogger.getCounter("message_send_errors");
        this.bytesSentCounter = producerStatsLogger.getCounter("bytes_sent");
        this.publishDelayLatencyStats = producerStatsLogger.getOpStatsLogger("producer_delay_latency");
        this.publishLatencyStats = producerStatsLogger.getOpStatsLogger("produce_latency");

        StatsLogger consumerStatsLogger = statsLogger.scope("consumer");
        this.messagesReceivedCounter = consumerStatsLogger.getCounter("messages_recv");
        this.bytesReceivedCounter = consumerStatsLogger.getCounter("bytes_recv");
        this.messagesAcknowledgedCounter = consumerStatsLogger.getCounter("messages_acknowledged");
        this.ackErrorCounter = consumerStatsLogger.getCounter("ack_errors");
        this.endToEndLatencyStats = consumerStatsLogger.getOpStatsLogger("e2e_latency");
    }

    public StatsLogger getStatsLogger() {
        return statsLogger;
    }

    public void recordProbeSuccess() {
        try {
            totalMessagesSent.increment();
        } finally {
            inFlightSends.decrement();
        }
    }

    public void recordMessageReceived(long payloadLength, long endToEndLatencyMicros) {
        messagesReceived.increment();
        totalMessagesReceived.increment();
        messagesReceivedCounter.inc();
        bytesReceived.add(payloadLength);
        bytesReceivedCounter.add(payloadLength);

        if (endToEndLatencyMicros > 0) {
            endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyStats.registerSuccessfulEvent(endToEndLatencyMicros, TimeUnit.MICROSECONDS);
        }
    }

    public PeriodStats toPeriodStats() {
        PeriodStats stats = new PeriodStats();

        stats.messagesSent = messagesSent.sumThenReset();
        stats.messageSendErrors = messageSendErrors.sumThenReset();
        stats.bytesSent = bytesSent.sumThenReset();

        stats.messagesReceived = messagesReceived.sumThenReset();
        stats.bytesReceived = bytesReceived.sumThenReset();
        stats.messagesAcknowledged = messagesAcknowledged.sumThenReset();
        stats.ackErrors = ackErrors.sumThenReset();

        stats.totalMessagesSent = totalMessagesSent.sum();
        stats.totalMessageSendErrors = totalMessageSendErrors.sum();
        stats.totalMessagesReceived = totalMessagesReceived.sum();
        stats.totalMessagesAcknowledged = totalMessagesAcknowledged.sum();
        stats.totalAckErrors = totalAckErrors.sum();
        stats.inFlightSends = inFlightSends.sum();
        stats.ackInFlight = ackInFlight.sum();
        stats.acknowledgementTrackingSupported = acknowledgementTrackingSupported;

        stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
        stats.publishDelayLatency = publishDelayLatencyRecorder.getIntervalHistogram();
        stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();
        return stats;
    }

    public CumulativeLatencies toCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
        latencies.publishDelayLatency = cumulativePublishDelayLatencyRecorder.getIntervalHistogram();
        latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
        return latencies;
    }

    public CountersStats toCountersStats() throws IOException {
        CountersStats stats = new CountersStats();
        long inFlightSendsBefore = inFlightSends.sum();
        long ackInFlightBefore = ackInFlight.sum();
        stats.messagesSent = totalMessagesSent.sum();
        stats.messageSendErrors = totalMessageSendErrors.sum();
        stats.messagesReceived = totalMessagesReceived.sum();
        stats.messagesAcknowledged = totalMessagesAcknowledged.sum();
        stats.ackErrors = totalAckErrors.sum();
        long ackInFlightAfter = ackInFlight.sum();
        long inFlightSendsAfter = inFlightSends.sum();
        stats.inFlightSends = Math.max(inFlightSendsBefore, inFlightSendsAfter);
        stats.ackInFlight = Math.max(ackInFlightBefore, ackInFlightAfter);
        stats.acknowledgementTrackingSupported = acknowledgementTrackingSupported;
        return stats;
    }

    public void resetLatencies() {
        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        publishDelayLatencyRecorder.reset();
        cumulativePublishDelayLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
    }

    public void resetMeasurement() {
        resetLatencies();

        messagesSent.reset();
        messageSendErrors.reset();
        bytesSent.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        messagesAcknowledged.reset();
        ackErrors.reset();
        totalMessagesSent.reset();
        totalMessageSendErrors.reset();
        totalMessagesReceived.reset();
        totalMessagesAcknowledged.reset();
        totalAckErrors.reset();
    }

    public void reset() {
        resetMeasurement();
        inFlightSends.reset();
        ackInFlight.reset();
        acknowledgementTrackingSupported = false;
    }

    public void recordProducerFailure() {
        try {
            messageSendErrors.increment();
            totalMessageSendErrors.increment();
            messageSendErrorCounter.inc();
        } finally {
            inFlightSends.decrement();
        }
    }

    public void recordProducerSuccess(
            long payloadLength, long intendedSendTimeNs, long sendTimeNs, long nowNs) {
        try {
            messagesSent.increment();
            totalMessagesSent.increment();
            messagesSentCounter.inc();
            bytesSent.add(payloadLength);
            bytesSentCounter.add(payloadLength);

            final long latencyMicros =
                    Math.min(highestTrackableValue, TimeUnit.NANOSECONDS.toMicros(nowNs - sendTimeNs));
            publishLatencyRecorder.recordValue(latencyMicros);
            cumulativePublishLatencyRecorder.recordValue(latencyMicros);
            publishLatencyStats.registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);

            final long sendDelayMicros =
                    Math.min(
                            highestTrackableValue,
                            TimeUnit.NANOSECONDS.toMicros(sendTimeNs - intendedSendTimeNs));
            publishDelayLatencyRecorder.recordValue(sendDelayMicros);
            cumulativePublishDelayLatencyRecorder.recordValue(sendDelayMicros);
            publishDelayLatencyStats.registerSuccessfulEvent(sendDelayMicros, TimeUnit.MICROSECONDS);
        } finally {
            inFlightSends.decrement();
        }
    }

    public void recordProducerStarted() {
        inFlightSends.increment();
    }

    public void recordAcknowledgementStarted() {
        acknowledgementTrackingSupported = true;
        ackInFlight.increment();
    }

    public void recordAcknowledgementCompleted(Throwable error) {
        try {
            if (error == null) {
                messagesAcknowledged.increment();
                totalMessagesAcknowledged.increment();
                messagesAcknowledgedCounter.inc();
            } else {
                ackErrors.increment();
                totalAckErrors.increment();
                ackErrorCounter.inc();
            }
        } finally {
            ackInFlight.decrement();
        }
    }
}
