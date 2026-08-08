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

import static java.util.concurrent.TimeUnit.MINUTES;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.benchmark.driver.DriverRuntimeInfo;
import io.openmessaging.benchmark.driver.RunConfiguration;
import io.openmessaging.benchmark.utils.PaddingDecimalFormat;
import io.openmessaging.benchmark.utils.SeedDerivation;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.PayloadPoolFactory;
import io.openmessaging.benchmark.utils.payload.PayloadReader;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PayloadSpec;
import io.openmessaging.benchmark.worker.commands.PayloadSpec.PayloadMode;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadGenerator implements AutoCloseable {

    private static final int REQUIRED_BROKER_BACKLOG_ZERO_POLLS = 2;
    private static final long BROKER_BACKLOG_POLL_INTERVAL_MILLIS = 1_000;

    private final String driverName;
    private final RunConfiguration run;
    private final Workload workload;
    private final Worker worker;
    private DriverRuntimeInfo runtimeInfo;
    private final List<TopicSubscription> backlogTargets = new ArrayList<>();
    private final List<String> assignmentFingerprint = new ArrayList<>();

    private final ExecutorService executor =
            Executors.newCachedThreadPool(new DefaultThreadFactory("messaging-benchmark"));

    private volatile boolean runCompleted = false;
    private volatile boolean needToWaitForBacklogDraining = false;
    private volatile Throwable runFailure;
    private volatile long requestedBacklogBytes;
    private volatile long backlogAtDrainStartMessages;
    private volatile double backlogBuildDurationSeconds;
    private volatile double backlogDrainDurationSeconds;
    private volatile double averageDrainRateMessagesPerSecond;
    private volatile double peakDrainRateMessagesPerSecond;
    private volatile long postDrainBacklogMessages;
    private volatile Long brokerBacklogAtDrainStartMessages;
    private volatile Long brokerBacklogAfterDrainMessages;
    private volatile String backlogPhase = "NOT_APPLICABLE";

    private volatile String measurementStartedAt;
    private volatile long measurementStartedAtNanos;
    private volatile double measurementDurationSeconds;
    private volatile boolean warmupDrainApplied;
    private volatile double warmupDrainDurationSeconds;
    private volatile long warmupDrainMessagesSent;
    private volatile long warmupDrainMessagesReceived;
    private volatile long warmupDrainMessageSendErrors;
    private volatile long warmupDrainInFlightSends;
    private volatile long warmupDrainMessagesAcknowledged;
    private volatile long warmupDrainAckErrors;
    private volatile long warmupDrainAckInFlight;
    private volatile boolean warmupDrainAcknowledgementTrackingSupported;
    private volatile long warmupDrainBacklogMessages;
    private volatile Long warmupDrainBrokerBacklogMessages;
    private volatile int warmupDrainBrokerBacklogZeroPolls;

    private volatile String measurementEndedAt;
    private volatile String measurementCompletedAt;
    private volatile boolean measurementDrainApplied;
    private volatile double measurementDrainDurationSeconds;
    private volatile long measurementDrainMessagesSent;
    private volatile long measurementDrainMessagesReceived;
    private volatile long measurementDrainMessageSendErrors;
    private volatile long measurementDrainInFlightSends;
    private volatile long measurementDrainMessagesAcknowledged;
    private volatile long measurementDrainAckErrors;
    private volatile long measurementDrainAckInFlight;
    private volatile boolean measurementDrainAcknowledgementTrackingSupported;
    private volatile long measurementDrainBacklogMessages;
    private volatile Long measurementDrainBrokerBacklogMessages;
    private volatile int measurementDrainBrokerBacklogZeroPolls;

    private volatile double targetPublishRate;

    public WorkloadGenerator(String driverName, Workload workload, Worker worker) {
        this(driverName, null, workload, worker);
    }

    public WorkloadGenerator(
            String driverName, RunConfiguration run, Workload workload, Worker worker) {
        this.driverName = driverName;
        this.run = run;
        this.workload = workload;
        this.worker = worker;
        workload.validate();
        validateRunConfiguration(run, workload);

        if (workload.consumerBacklogSizeGB > 0 && workload.producerRate == 0) {
            throw new IllegalArgumentException(
                    "Cannot probe producer sustainable rate when building backlog");
        }
    }

    public TestResult run() throws Exception {
        runtimeInfo = worker.getDriverRuntimeInfo();
        validateRuntimeInfo(runtimeInfo);
        Timer timer = new Timer();
        List<String> topics =
                worker.createTopics(
                        new TopicsInfo(
                                workload.topics, workload.partitionsPerTopic, run == null ? null : run.seed));
        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());

        createConsumers(topics);
        createProducers(topics);

        ensureTopicsAreReady();

        if (workload.producerRate > 0) {
            targetPublishRate = workload.producerRate;
        } else {
            // Producer rate is 0 and we need to discover the sustainable rate
            targetPublishRate = 10000;

            executor.execute(
                    () -> {
                        // Run background controller to adjust rate
                        try {
                            findMaximumSustainableRate(targetPublishRate);
                        } catch (Throwable e) {
                            runFailure = e;
                            log.warn("Failure in finding max sustainable rate", e);
                        }
                    });
        }

        ProducerWorkAssignment producerWorkAssignment = new ProducerWorkAssignment();
        producerWorkAssignment.keyDistributorType = workload.keyDistributor;
        producerWorkAssignment.publishRate = targetPublishRate;
        producerWorkAssignment.payloadSelectionSeed = deriveSeed("worker-payload-selection", 0);

        if (workload.useRandomizedPayloads) {
            PayloadSpec spec = new PayloadSpec();
            spec.mode = PayloadMode.RANDOMIZED;
            spec.messageSize = workload.messageSize;
            spec.randomBytesRatio = workload.randomBytesRatio;
            spec.poolSize = workload.randomizedPayloadPoolSize;
            spec.seed = deriveSeed("payload-pool", 0);
            List<byte[]> payloadPool = PayloadPoolFactory.create(spec);
            spec.expectedSha256 = PayloadPoolFactory.checksum(payloadPool);
            producerWorkAssignment.payloadSpec = spec;
        } else {
            PayloadReader payloadReader = new FilePayloadReader(workload.messageSize);
            PayloadSpec spec = new PayloadSpec();
            spec.mode = PayloadMode.INLINE;
            spec.messageSize = workload.messageSize;
            spec.inlinePayload = payloadReader.load(workload.payloadFile);
            spec.expectedSha256 =
                    PayloadPoolFactory.checksum(java.util.Collections.singletonList(spec.inlinePayload));
            producerWorkAssignment.payloadSpec = spec;
        }

        worker.startLoad(producerWorkAssignment);

        if (workload.warmupDurationMinutes > 0) {
            log.info("----- Starting warm-up traffic ({}m) ------", workload.warmupDurationMinutes);
            printAndCollectStats(workload.warmupDurationMinutes, TimeUnit.MINUTES);
        }

        prepareMeasurementWindow();

        if (workload.consumerBacklogSizeGB > 0) {
            executor.execute(
                    () -> {
                        try {
                            buildAndDrainBacklog(workload.testDurationMinutes);
                        } catch (Throwable e) {
                            runFailure = e;
                            log.error("Failure in backlog phase", e);
                        }
                    });
        }

        log.info("----- Starting benchmark traffic ({}m)------", workload.testDurationMinutes);

        TestResult result = printAndCollectStats(workload.testDurationMinutes, TimeUnit.MINUTES);
        completeMeasurementWindow();
        collectAggregatedLatencies(result);
        if (workload.consumerBacklogSizeGB > 0) {
            backlogPhase = "COMPLETE";
        }
        PayloadSpec effectivePayload = producerWorkAssignment.payloadSpec;
        result.payloadMode = effectivePayload.mode.name();
        result.payloadMessageSize = effectivePayload.messageSize;
        result.payloadRandomBytesRatio = effectivePayload.randomBytesRatio;
        result.payloadPoolSize = effectivePayload.poolSize;
        result.payloadSeed = effectivePayload.seed;
        result.payloadSha256 = effectivePayload.expectedSha256;
        result.assignmentSha256 = assignmentSha256();
        result.measurementStartedAt = measurementStartedAt;
        result.measurementDurationSeconds = measurementDurationSeconds;
        result.warmupDrainApplied = warmupDrainApplied;
        result.warmupDrainDurationSeconds = warmupDrainDurationSeconds;
        result.warmupDrainMessagesSent = warmupDrainMessagesSent;
        result.warmupDrainMessagesReceived = warmupDrainMessagesReceived;
        result.warmupDrainMessageSendErrors = warmupDrainMessageSendErrors;
        result.warmupDrainInFlightSends = warmupDrainInFlightSends;
        result.warmupDrainMessagesAcknowledged = warmupDrainMessagesAcknowledged;
        result.warmupDrainAckErrors = warmupDrainAckErrors;
        result.warmupDrainAckInFlight = warmupDrainAckInFlight;
        result.warmupDrainAcknowledgementTrackingSupported =
                warmupDrainAcknowledgementTrackingSupported;
        result.warmupDrainBacklogMessages = warmupDrainBacklogMessages;
        result.warmupDrainBrokerBacklogMessages = warmupDrainBrokerBacklogMessages;
        result.warmupDrainBrokerBacklogZeroPolls = warmupDrainBrokerBacklogZeroPolls;
        result.measurementEndedAt = measurementEndedAt;
        result.measurementCompletedAt = measurementCompletedAt;
        result.measurementDrainApplied = measurementDrainApplied;
        result.measurementDrainDurationSeconds = measurementDrainDurationSeconds;
        result.measurementDrainMessagesSent = measurementDrainMessagesSent;
        result.measurementDrainMessagesReceived = measurementDrainMessagesReceived;
        result.measurementDrainMessageSendErrors = measurementDrainMessageSendErrors;
        result.measurementDrainInFlightSends = measurementDrainInFlightSends;
        result.measurementDrainMessagesAcknowledged = measurementDrainMessagesAcknowledged;
        result.measurementDrainAckErrors = measurementDrainAckErrors;
        result.measurementDrainAckInFlight = measurementDrainAckInFlight;
        result.measurementDrainAcknowledgementTrackingSupported =
                measurementDrainAcknowledgementTrackingSupported;
        result.measurementDrainBacklogMessages = measurementDrainBacklogMessages;
        result.measurementDrainBrokerBacklogMessages = measurementDrainBrokerBacklogMessages;
        result.measurementDrainBrokerBacklogZeroPolls = measurementDrainBrokerBacklogZeroPolls;
        result.targetPublishRate = targetPublishRate;
        result.requestedBacklogBytes = requestedBacklogBytes;
        result.backlogAtDrainStartMessages = backlogAtDrainStartMessages;
        result.backlogBuildDurationSeconds = backlogBuildDurationSeconds;
        result.backlogDrainDurationSeconds = backlogDrainDurationSeconds;
        result.averageDrainRateMessagesPerSecond = averageDrainRateMessagesPerSecond;
        result.peakDrainRateMessagesPerSecond = peakDrainRateMessagesPerSecond;
        result.postDrainBacklogMessages = postDrainBacklogMessages;
        result.brokerBacklogAtDrainStartMessages = brokerBacklogAtDrainStartMessages;
        result.brokerBacklogAfterDrainMessages = brokerBacklogAfterDrainMessages;
        result.backlogPhase = backlogPhase;
        runCompleted = true;

        worker.stopAll();
        return result;
    }

    void prepareMeasurementWindow() throws IOException {
        if (workload.warmupDrainTimeoutSeconds == 0) {
            worker.resetStats();
            measurementStartedAt = Instant.now().toString();
            measurementStartedAtNanos = System.nanoTime();
            return;
        }

        warmupDrainApplied = true;
        long startedAtNanos = System.nanoTime();
        boolean boundaryPrepared = false;
        try {
            worker.pauseProducers();
            drainBoundary(DrainBoundary.WARMUP, workload.warmupDrainTimeoutSeconds);
            worker.resetStats();
            measurementStartedAt = Instant.now().toString();
            measurementStartedAtNanos = System.nanoTime();
            worker.resumeProducers();
            boundaryPrepared = true;
            log.info(
                    "Warm-up traffic drained in {} seconds; resetting measurement statistics",
                    warmupDrainDurationSeconds);
        } catch (InvalidBenchmarkRunException error) {
            throw error;
        } catch (IOException | RuntimeException error) {
            throw new InvalidBenchmarkRunException(
                    "failed to establish a clean pre-measurement boundary", error);
        } finally {
            if (!boundaryPrepared) {
                warmupDrainDurationSeconds = (System.nanoTime() - startedAtNanos) / 1_000_000_000.0;
                log.warn("Warm-up boundary failed; producers remain paused until worker shutdown");
            }
        }
    }

    void completeMeasurementWindow() throws IOException {
        if (workload.measurementDrainTimeoutSeconds == 0) {
            worker.pauseProducers();
            markMeasurementEnded();
            measurementCompletedAt = measurementEndedAt;
            return;
        }

        measurementDrainApplied = true;
        boolean boundaryCompleted = false;
        try {
            worker.pauseProducers();
            markMeasurementEnded();
            drainBoundary(DrainBoundary.MEASUREMENT, workload.measurementDrainTimeoutSeconds);
            measurementCompletedAt = Instant.now().toString();
            boundaryCompleted = true;
            log.info("Measurement traffic drained in {} seconds", measurementDrainDurationSeconds);
        } catch (InvalidBenchmarkRunException error) {
            throw error;
        } catch (IOException | RuntimeException error) {
            throw new InvalidBenchmarkRunException(
                    "failed to establish a clean final measurement boundary", error);
        } finally {
            if (!boundaryCompleted) {
                log.warn("Measurement boundary failed; producers remain paused until worker shutdown");
            }
        }
    }

    private void markMeasurementEnded() {
        measurementEndedAt = Instant.now().toString();
        if (measurementStartedAtNanos != 0) {
            measurementDurationSeconds =
                    (System.nanoTime() - measurementStartedAtNanos) / 1_000_000_000.0;
        }
    }

    private void drainBoundary(DrainBoundary boundary, int timeoutSeconds) throws IOException {
        long startedAtNanos = System.nanoTime();
        long deadlineNanos = startedAtNanos + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        int consecutiveBrokerBacklogZeroPolls = 0;
        long nextLogNanos = startedAtNanos;
        try {
            while (true) {
                CountersStats stats = worker.getCountersStats();
                long expectedReceives =
                        Math.multiplyExact(stats.messagesSent, (long) workload.subscriptionsPerTopic);
                long backlog = Math.max(0, expectedReceives - stats.messagesReceived);
                if (run != null && !stats.acknowledgementTrackingSupported) {
                    recordDrainSnapshot(boundary, stats, backlog, null, 0);
                    throw new InvalidBenchmarkRunException(
                            boundary.label + " does not expose acknowledgement completion");
                }
                if (stats.messageSendErrors > 0 || stats.ackErrors > 0) {
                    recordDrainSnapshot(boundary, stats, backlog, null, 0);
                    throw new InvalidBenchmarkRunException(
                            String.format(
                                    "%s had sendErrors=%d and ackErrors=%d",
                                    boundary.label, stats.messageSendErrors, stats.ackErrors));
                }
                if (stats.messagesReceived > expectedReceives) {
                    recordDrainSnapshot(boundary, stats, backlog, null, 0);
                    throw new InvalidBenchmarkRunException(
                            String.format(
                                    "%s received %d deliveries for %d expected messages; possible redelivery",
                                    boundary.label, stats.messagesReceived, expectedReceives));
                }
                boolean acknowledgementsComplete =
                        !stats.acknowledgementTrackingSupported
                                || (stats.ackInFlight == 0 && stats.messagesAcknowledged == stats.messagesReceived);
                if (stats.acknowledgementTrackingSupported
                        && stats.ackInFlight == 0
                        && stats.messagesAcknowledged != stats.messagesReceived) {
                    recordDrainSnapshot(boundary, stats, backlog, null, 0);
                    throw new InvalidBenchmarkRunException(
                            String.format(
                                    "%s has %d received messages but %d completed acknowledgements",
                                    boundary.label, stats.messagesReceived, stats.messagesAcknowledged));
                }

                boolean localDrainComplete =
                        stats.inFlightSends == 0 && backlog == 0 && acknowledgementsComplete;
                Long brokerBacklog = null;
                if (localDrainComplete) {
                    brokerBacklog = readBrokerBacklog();
                    if (brokerBacklog == null) {
                        consecutiveBrokerBacklogZeroPolls = 0;
                    } else if (brokerBacklog == 0) {
                        consecutiveBrokerBacklogZeroPolls++;
                    } else {
                        consecutiveBrokerBacklogZeroPolls = 0;
                    }
                } else {
                    consecutiveBrokerBacklogZeroPolls = 0;
                }

                recordDrainSnapshot(
                        boundary, stats, backlog, brokerBacklog, consecutiveBrokerBacklogZeroPolls);

                boolean brokerDrainComplete =
                        brokerBacklog == null
                                || consecutiveBrokerBacklogZeroPolls >= REQUIRED_BROKER_BACKLOG_ZERO_POLLS;
                if (localDrainComplete && brokerDrainComplete) {
                    break;
                }

                long now = System.nanoTime();
                if (now >= deadlineNanos) {
                    throw new InvalidBenchmarkRunException(
                            String.format(
                                    "timed out draining %s after %d seconds: sent=%d, received=%d, "
                                            + "acknowledged=%d, sendErrors=%d, ackErrors=%d, "
                                            + "sendInFlight=%d, ackInFlight=%d, backlog=%d, "
                                            + "brokerBacklog=%s, brokerZeroPolls=%d",
                                    boundary.label,
                                    timeoutSeconds,
                                    stats.messagesSent,
                                    stats.messagesReceived,
                                    stats.messagesAcknowledged,
                                    stats.messageSendErrors,
                                    stats.ackErrors,
                                    stats.inFlightSends,
                                    stats.ackInFlight,
                                    backlog,
                                    brokerBacklog,
                                    consecutiveBrokerBacklogZeroPolls));
                }
                if (now >= nextLogNanos) {
                    log.info(
                            "Draining {} -- Sent: {}, Received: {}, Acknowledged: {}, "
                                    + "Send errors: {}, Ack errors: {}, Send in-flight: {}, "
                                    + "Ack in-flight: {}, Backlog: {}, Broker backlog: {}, "
                                    + "Broker zero polls: {}/{}",
                            boundary.label,
                            stats.messagesSent,
                            stats.messagesReceived,
                            stats.messagesAcknowledged,
                            stats.messageSendErrors,
                            stats.ackErrors,
                            stats.inFlightSends,
                            stats.ackInFlight,
                            backlog,
                            brokerBacklog,
                            consecutiveBrokerBacklogZeroPolls,
                            REQUIRED_BROKER_BACKLOG_ZERO_POLLS);
                    nextLogNanos = now + TimeUnit.SECONDS.toNanos(5);
                }
                try {
                    Thread.sleep(localDrainComplete ? BROKER_BACKLOG_POLL_INTERVAL_MILLIS : 100);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while draining " + boundary.label, error);
                }
            }
        } finally {
            double durationSeconds = (System.nanoTime() - startedAtNanos) / 1_000_000_000.0;
            if (boundary == DrainBoundary.WARMUP) {
                warmupDrainDurationSeconds = durationSeconds;
            } else {
                measurementDrainDurationSeconds = durationSeconds;
            }
        }
    }

    private void recordDrainSnapshot(
            DrainBoundary boundary,
            CountersStats stats,
            long backlog,
            Long brokerBacklog,
            int brokerBacklogZeroPolls) {
        if (boundary == DrainBoundary.WARMUP) {
            warmupDrainMessagesSent = stats.messagesSent;
            warmupDrainMessagesReceived = stats.messagesReceived;
            warmupDrainMessageSendErrors = stats.messageSendErrors;
            warmupDrainInFlightSends = stats.inFlightSends;
            warmupDrainMessagesAcknowledged = stats.messagesAcknowledged;
            warmupDrainAckErrors = stats.ackErrors;
            warmupDrainAckInFlight = stats.ackInFlight;
            warmupDrainAcknowledgementTrackingSupported = stats.acknowledgementTrackingSupported;
            warmupDrainBacklogMessages = backlog;
            warmupDrainBrokerBacklogMessages = brokerBacklog;
            warmupDrainBrokerBacklogZeroPolls = brokerBacklogZeroPolls;
            return;
        }

        measurementDrainMessagesSent = stats.messagesSent;
        measurementDrainMessagesReceived = stats.messagesReceived;
        measurementDrainMessageSendErrors = stats.messageSendErrors;
        measurementDrainInFlightSends = stats.inFlightSends;
        measurementDrainMessagesAcknowledged = stats.messagesAcknowledged;
        measurementDrainAckErrors = stats.ackErrors;
        measurementDrainAckInFlight = stats.ackInFlight;
        measurementDrainAcknowledgementTrackingSupported = stats.acknowledgementTrackingSupported;
        measurementDrainBacklogMessages = backlog;
        measurementDrainBrokerBacklogMessages = brokerBacklog;
        measurementDrainBrokerBacklogZeroPolls = brokerBacklogZeroPolls;
    }

    String getMeasurementStartedAt() {
        return measurementStartedAt;
    }

    void copyBoundaryEvidenceTo(RunManifest manifest) {
        manifest.measurementStartedAt = measurementStartedAt;
        manifest.measurementDurationSeconds = measurementDurationSeconds;
        manifest.warmupDrainApplied = warmupDrainApplied;
        manifest.warmupDrainDurationSeconds = warmupDrainDurationSeconds;
        manifest.warmupDrainMessagesSent = warmupDrainMessagesSent;
        manifest.warmupDrainMessagesReceived = warmupDrainMessagesReceived;
        manifest.warmupDrainMessageSendErrors = warmupDrainMessageSendErrors;
        manifest.warmupDrainInFlightSends = warmupDrainInFlightSends;
        manifest.warmupDrainMessagesAcknowledged = warmupDrainMessagesAcknowledged;
        manifest.warmupDrainAckErrors = warmupDrainAckErrors;
        manifest.warmupDrainAckInFlight = warmupDrainAckInFlight;
        manifest.warmupDrainAcknowledgementTrackingSupported =
                warmupDrainAcknowledgementTrackingSupported;
        manifest.warmupDrainBacklogMessages = warmupDrainBacklogMessages;
        manifest.warmupDrainBrokerBacklogMessages = warmupDrainBrokerBacklogMessages;
        manifest.warmupDrainBrokerBacklogZeroPolls = warmupDrainBrokerBacklogZeroPolls;
        manifest.measurementEndedAt = measurementEndedAt;
        manifest.measurementCompletedAt = measurementCompletedAt;
        manifest.measurementDrainApplied = measurementDrainApplied;
        manifest.measurementDrainDurationSeconds = measurementDrainDurationSeconds;
        manifest.measurementDrainMessagesSent = measurementDrainMessagesSent;
        manifest.measurementDrainMessagesReceived = measurementDrainMessagesReceived;
        manifest.measurementDrainMessageSendErrors = measurementDrainMessageSendErrors;
        manifest.measurementDrainInFlightSends = measurementDrainInFlightSends;
        manifest.measurementDrainMessagesAcknowledged = measurementDrainMessagesAcknowledged;
        manifest.measurementDrainAckErrors = measurementDrainAckErrors;
        manifest.measurementDrainAckInFlight = measurementDrainAckInFlight;
        manifest.measurementDrainAcknowledgementTrackingSupported =
                measurementDrainAcknowledgementTrackingSupported;
        manifest.measurementDrainBacklogMessages = measurementDrainBacklogMessages;
        manifest.measurementDrainBrokerBacklogMessages = measurementDrainBrokerBacklogMessages;
        manifest.measurementDrainBrokerBacklogZeroPolls = measurementDrainBrokerBacklogZeroPolls;
    }

    private enum DrainBoundary {
        WARMUP("pre-measurement traffic"),
        MEASUREMENT("measurement traffic");

        private final String label;

        DrainBoundary(String label) {
            this.label = label;
        }
    }

    private void ensureTopicsAreReady() throws IOException {
        log.info("Waiting for consumers to be ready");
        // This is work around the fact that there's no way to have a consumer ready in Kafka without
        // first publishing
        // some message on the topic, which will then trigger the partitions assignment to the consumers

        int expectedMessages = workload.topics * workload.subscriptionsPerTopic;

        // In this case we just publish 1 message and then wait for consumers to receive the data
        try {
            worker.probeProducers();
        } catch (IOException | RuntimeException error) {
            throw new InvalidBenchmarkRunException("readiness probe send failed; run is invalid", error);
        }

        long start = System.currentTimeMillis();
        long end = start + 60 * 1000;
        while (System.currentTimeMillis() < end) {
            CountersStats stats = worker.getCountersStats();

            log.info(
                    "Waiting for topics to be ready -- Sent: {}, Received: {}",
                    stats.messagesSent,
                    stats.messagesReceived);
            if (stats.messagesReceived < expectedMessages) {
                try {
                    Thread.sleep(2_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }
        }

        if (System.currentTimeMillis() >= end) {
            throw new InvalidBenchmarkRunException(
                    "Timed out waiting for consumers to be ready; run is invalid");
        } else {
            log.info("All consumers are ready");
        }
    }

    /**
     * Adjust the publish rate to a level that is sustainable, meaning that we can consume all the
     * messages that are being produced.
     *
     * @param currentRate
     */
    private void findMaximumSustainableRate(double currentRate) throws IOException {
        CountersStats stats = worker.getCountersStats();

        int controlPeriodMillis = 3000;
        long lastControlTimestamp = System.nanoTime();

        RateController rateController = new RateController();

        while (!runCompleted) {
            // Check every few seconds and adjust the rate
            try {
                Thread.sleep(controlPeriodMillis);
            } catch (InterruptedException e) {
                return;
            }

            // Consider multiple copies when using multiple subscriptions
            stats = worker.getCountersStats();
            long currentTime = System.nanoTime();
            long periodNanos = currentTime - lastControlTimestamp;

            lastControlTimestamp = currentTime;

            currentRate =
                    rateController.nextRate(
                            currentRate, periodNanos, stats.messagesSent, stats.messagesReceived);
            worker.adjustPublishRate(currentRate);
        }
    }

    @Override
    public void close() throws Exception {
        worker.stopAll();
        executor.shutdownNow();
    }

    private void createConsumers(List<String> topics) throws IOException {
        ConsumerAssignment consumerAssignment = new ConsumerAssignment();
        backlogTargets.clear();
        assignmentFingerprint.clear();

        for (String topic : topics) {
            for (int i = 0; i < workload.subscriptionsPerTopic; i++) {
                String subscriptionName =
                        String.format("sub-%03d-%016x", i, deriveSeed("subscription-name", i));
                backlogTargets.add(new TopicSubscription(topic, subscriptionName));
                for (int j = 0; j < workload.consumerPerSubscription; j++) {
                    consumerAssignment.topicsSubscriptions.add(
                            new TopicSubscription(topic, subscriptionName));
                }
            }
        }

        Collections.shuffle(
                consumerAssignment.topicsSubscriptions, new Random(deriveSeed("consumer-assignment", 0)));
        for (TopicSubscription assignment : consumerAssignment.topicsSubscriptions) {
            assignmentFingerprint.add("consumer\0" + assignment.topic + "\0" + assignment.subscription);
        }

        Timer timer = new Timer();

        worker.createConsumers(consumerAssignment);
        log.info(
                "Created {} consumers in {} ms",
                consumerAssignment.topicsSubscriptions.size(),
                timer.elapsedMillis());
    }

    private void createProducers(List<String> topics) throws IOException {
        List<String> fullListOfTopics = new ArrayList<>();

        // Add the topic multiple times, one for each producer
        for (int i = 0; i < workload.producersPerTopic; i++) {
            fullListOfTopics.addAll(topics);
        }

        Collections.shuffle(fullListOfTopics, new Random(deriveSeed("producer-assignment", 0)));
        for (String topic : fullListOfTopics) {
            assignmentFingerprint.add("producer\0" + topic);
        }

        Timer timer = new Timer();

        worker.createProducers(fullListOfTopics, workload);
        log.info("Created {} producers in {} ms", fullListOfTopics.size(), timer.elapsedMillis());
    }

    private void buildAndDrainBacklog(int testDurationMinutes) throws IOException {
        Timer timer = new Timer();
        backlogPhase = "PREPARE_SUBSCRIPTION";
        log.info("Stopping all consumers to build backlog");
        worker.pauseConsumers();

        this.needToWaitForBacklogDraining = true;
        backlogPhase = "BUILD_BACKLOG";

        long requestedBacklogSize = workload.consumerBacklogSizeGB * 1024L * 1024L * 1024L;
        requestedBacklogBytes = requestedBacklogSize;
        long buildStartNanos = System.nanoTime();

        while (true) {
            CountersStats stats = worker.getCountersStats();
            long currentBacklogSize =
                    (workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived)
                            * workload.messageSize;

            if (currentBacklogSize >= requestedBacklogSize) {
                backlogAtDrainStartMessages =
                        workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived;
                brokerBacklogAtDrainStartMessages = readBrokerBacklog();
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        backlogBuildDurationSeconds = (System.nanoTime() - buildStartNanos) / 1_000_000_000.0;
        log.info("--- Completed backlog build in {} s ---", timer.elapsedSeconds());
        timer = new Timer();
        log.info("--- Start draining backlog ---");

        backlogPhase = "DRAIN_BACKLOG";
        worker.resumeConsumers();
        long drainStartNanos = System.nanoTime();
        long previousBacklog = backlogAtDrainStartMessages;
        long previousNanos = drainStartNanos;
        long drainedMessages = 0;

        long backlogMessageCapacity = requestedBacklogSize / workload.messageSize;
        long backlogEmptyLevel = (long) ((1.0 - workload.backlogDrainRatio) * backlogMessageCapacity);
        final long minBacklog = Math.max(1000L, backlogEmptyLevel);

        while (true) {
            CountersStats stats = worker.getCountersStats();
            long currentBacklog =
                    workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived;
            long nowNanos = System.nanoTime();
            long elapsedNanos = nowNanos - previousNanos;
            if (elapsedNanos > 0 && currentBacklog < previousBacklog) {
                double rate = (previousBacklog - currentBacklog) / (elapsedNanos / 1_000_000_000.0);
                peakDrainRateMessagesPerSecond = Math.max(peakDrainRateMessagesPerSecond, rate);
                drainedMessages += previousBacklog - currentBacklog;
            }
            previousBacklog = currentBacklog;
            previousNanos = nowNanos;
            if (currentBacklog <= minBacklog) {
                postDrainBacklogMessages = currentBacklog;
                brokerBacklogAfterDrainMessages = readBrokerBacklog();
                backlogDrainDurationSeconds = (nowNanos - drainStartNanos) / 1_000_000_000.0;
                averageDrainRateMessagesPerSecond =
                        backlogDrainDurationSeconds > 0 ? drainedMessages / backlogDrainDurationSeconds : 0.0;
                log.info("--- Completed backlog draining in {} s ---", timer.elapsedSeconds());

                try {
                    Thread.sleep(MINUTES.toMillis(testDurationMinutes));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                needToWaitForBacklogDraining = false;
                backlogPhase = "POST_DRAIN_STEADY";
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Long readBrokerBacklog() throws IOException {
        long total = 0;
        for (TopicSubscription target : backlogTargets) {
            long backlog = worker.getSubscriptionBacklog(target.topic, target.subscription);
            if (backlog < 0) {
                if (run != null) {
                    throw new IOException(
                            "driver does not expose broker backlog for formal run: "
                                    + target.topic
                                    + "/"
                                    + target.subscription);
                }
                return null;
            }
            total = Math.addExact(total, backlog);
        }
        return total;
    }

    private String assignmentSha256() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String assignment : assignmentFingerprint) {
                digest.update(assignment.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) '\n');
            }
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) {
                result.append(String.format("%02x", value & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM does not provide SHA-256", e);
        }
    }

    @SuppressWarnings({"checkstyle:LineLength", "checkstyle:MethodLength"})
    private TestResult printAndCollectStats(long testDurations, TimeUnit unit) throws IOException {
        long startTime = System.nanoTime();

        // Print report stats
        long oldTime = System.nanoTime();

        long testEndTime = testDurations > 0 ? startTime + unit.toNanos(testDurations) : Long.MAX_VALUE;

        TestResult result = new TestResult();
        result.run = run;
        result.runtimeInfo = runtimeInfo;
        result.workload = workload.name;
        result.driver = driverName;
        result.topics = workload.topics;
        result.partitions = workload.partitionsPerTopic;
        result.messageSize = workload.messageSize;
        result.producersPerTopic = workload.producersPerTopic;
        result.consumersPerTopic = workload.consumerPerSubscription;

        while (true) {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(workload.statsIntervalSeconds));
            } catch (InterruptedException e) {
                break;
            }

            if (runFailure != null) {
                throw new IOException("benchmark background phase failed", runFailure);
            }

            PeriodStats stats = worker.getPeriodStats();

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double publishRate = stats.messagesSent / elapsed;
            double publishThroughput = stats.bytesSent / elapsed / 1024 / 1024;
            double errorRate = stats.messageSendErrors / elapsed;

            double consumeRate = stats.messagesReceived / elapsed;
            double consumeThroughput = stats.bytesReceived / elapsed / 1024 / 1024;

            long currentBacklog =
                    Math.max(
                            0L,
                            workload.subscriptionsPerTopic * stats.totalMessagesSent
                                    - stats.totalMessagesReceived);

            log.info(
                    "Pub rate {} msg/s / {} MB/s | Pub err {} err/s | Cons rate {} msg/s / {} MB/s | Backlog: {} K | Pub Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {} | Pub Delay Latency (us) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
                    rateFormat.format(publishRate),
                    throughputFormat.format(publishThroughput),
                    rateFormat.format(errorRate),
                    rateFormat.format(consumeRate),
                    throughputFormat.format(consumeThroughput),
                    dec.format(currentBacklog / 1000.0), //
                    dec.format(microsToMillis(stats.publishLatency.getMean())),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(50))),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99))),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9))),
                    throughputFormat.format(microsToMillis(stats.publishLatency.getMaxValue())),
                    dec.format(stats.publishDelayLatency.getMean()),
                    dec.format(stats.publishDelayLatency.getValueAtPercentile(50)),
                    dec.format(stats.publishDelayLatency.getValueAtPercentile(99)),
                    dec.format(stats.publishDelayLatency.getValueAtPercentile(99.9)),
                    throughputFormat.format(stats.publishDelayLatency.getMaxValue()));

            result.publishRate.add(publishRate);
            result.publishErrorRate.add(errorRate);
            result.consumeRate.add(consumeRate);
            result.backlog.add(currentBacklog);
            result.publishLatencyAvg.add(microsToMillis(stats.publishLatency.getMean()));
            result.publishLatency50pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(50)));
            result.publishLatency75pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(75)));
            result.publishLatency95pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(95)));
            result.publishLatency99pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99)));
            result.publishLatency999pct.add(
                    microsToMillis(stats.publishLatency.getValueAtPercentile(99.9)));
            result.publishLatency9999pct.add(
                    microsToMillis(stats.publishLatency.getValueAtPercentile(99.99)));
            result.publishLatencyMax.add(microsToMillis(stats.publishLatency.getMaxValue()));

            result.publishDelayLatencyAvg.add(stats.publishDelayLatency.getMean());
            result.publishDelayLatency50pct.add(stats.publishDelayLatency.getValueAtPercentile(50));
            result.publishDelayLatency75pct.add(stats.publishDelayLatency.getValueAtPercentile(75));
            result.publishDelayLatency95pct.add(stats.publishDelayLatency.getValueAtPercentile(95));
            result.publishDelayLatency99pct.add(stats.publishDelayLatency.getValueAtPercentile(99));
            result.publishDelayLatency999pct.add(stats.publishDelayLatency.getValueAtPercentile(99.9));
            result.publishDelayLatency9999pct.add(stats.publishDelayLatency.getValueAtPercentile(99.99));
            result.publishDelayLatencyMax.add(stats.publishDelayLatency.getMaxValue());

            result.endToEndLatencyAvg.add(microsToMillis(stats.endToEndLatency.getMean()));
            result.endToEndLatency50pct.add(
                    microsToMillis(stats.endToEndLatency.getValueAtPercentile(50)));
            result.endToEndLatency75pct.add(
                    microsToMillis(stats.endToEndLatency.getValueAtPercentile(75)));
            result.endToEndLatency95pct.add(
                    microsToMillis(stats.endToEndLatency.getValueAtPercentile(95)));
            result.endToEndLatency99pct.add(
                    microsToMillis(stats.endToEndLatency.getValueAtPercentile(99)));
            result.endToEndLatency999pct.add(
                    microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9)));
            result.endToEndLatency9999pct.add(
                    microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.99)));
            result.endToEndLatencyMax.add(microsToMillis(stats.endToEndLatency.getMaxValue()));

            PeriodSample sample = new PeriodSample();
            sample.timestamp = Instant.now().toString();
            sample.elapsedSeconds = (now - startTime) / 1_000_000_000L;
            sample.messagesSent = stats.messagesSent;
            sample.messagesReceived = stats.messagesReceived;
            sample.messageSendErrors = stats.messageSendErrors;
            sample.bytesSent = stats.bytesSent;
            sample.bytesReceived = stats.bytesReceived;
            sample.inFlightSends = stats.inFlightSends;
            sample.messagesAcknowledged = stats.messagesAcknowledged;
            sample.ackErrors = stats.ackErrors;
            sample.ackInFlight = stats.ackInFlight;
            sample.backlog = currentBacklog;
            sample.publishRate = publishRate;
            sample.publishThroughputMiB = publishThroughput;
            sample.consumeRate = consumeRate;
            sample.consumeThroughputMiB = consumeThroughput;
            sample.publishErrorRate = errorRate;
            sample.publishLatencyP99Ms = microsToMillis(stats.publishLatency.getValueAtPercentile(99));
            sample.endToEndLatencyP99Ms = microsToMillis(stats.endToEndLatency.getValueAtPercentile(99));
            result.samples.add(sample);

            if (now >= testEndTime && !needToWaitForBacklogDraining) {
                break;
            }

            oldTime = now;
        }

        return result;
    }

    @SuppressWarnings({"checkstyle:LineLength", "checkstyle:MethodLength"})
    private void collectAggregatedLatencies(TestResult result) throws IOException {
        CumulativeLatencies aggregated = worker.getCumulativeLatencies();
        log.info(
                "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {} | Pub Delay (us)  avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}",
                dec.format(aggregated.publishLatency.getMean() / 1000.0),
                dec.format(aggregated.publishLatency.getValueAtPercentile(50) / 1000.0),
                dec.format(aggregated.publishLatency.getValueAtPercentile(95) / 1000.0),
                dec.format(aggregated.publishLatency.getValueAtPercentile(99) / 1000.0),
                dec.format(aggregated.publishLatency.getValueAtPercentile(99.9) / 1000.0),
                dec.format(aggregated.publishLatency.getValueAtPercentile(99.99) / 1000.0),
                throughputFormat.format(aggregated.publishLatency.getMaxValue() / 1000.0),
                dec.format(aggregated.publishDelayLatency.getMean()),
                dec.format(aggregated.publishDelayLatency.getValueAtPercentile(50)),
                dec.format(aggregated.publishDelayLatency.getValueAtPercentile(95)),
                dec.format(aggregated.publishDelayLatency.getValueAtPercentile(99)),
                dec.format(aggregated.publishDelayLatency.getValueAtPercentile(99.9)),
                dec.format(aggregated.publishDelayLatency.getValueAtPercentile(99.99)),
                throughputFormat.format(aggregated.publishDelayLatency.getMaxValue()));

        result.aggregatedPublishLatencyAvg = aggregated.publishLatency.getMean() / 1000.0;
        result.aggregatedPublishLatency50pct =
                aggregated.publishLatency.getValueAtPercentile(50) / 1000.0;
        result.aggregatedPublishLatency75pct =
                aggregated.publishLatency.getValueAtPercentile(75) / 1000.0;
        result.aggregatedPublishLatency95pct =
                aggregated.publishLatency.getValueAtPercentile(95) / 1000.0;
        result.aggregatedPublishLatency99pct =
                aggregated.publishLatency.getValueAtPercentile(99) / 1000.0;
        result.aggregatedPublishLatency999pct =
                aggregated.publishLatency.getValueAtPercentile(99.9) / 1000.0;
        result.aggregatedPublishLatency9999pct =
                aggregated.publishLatency.getValueAtPercentile(99.99) / 1000.0;
        result.aggregatedPublishLatencyMax = aggregated.publishLatency.getMaxValue() / 1000.0;

        result.aggregatedPublishDelayLatencyAvg = aggregated.publishDelayLatency.getMean();
        result.aggregatedPublishDelayLatency50pct =
                aggregated.publishDelayLatency.getValueAtPercentile(50);
        result.aggregatedPublishDelayLatency75pct =
                aggregated.publishDelayLatency.getValueAtPercentile(75);
        result.aggregatedPublishDelayLatency95pct =
                aggregated.publishDelayLatency.getValueAtPercentile(95);
        result.aggregatedPublishDelayLatency99pct =
                aggregated.publishDelayLatency.getValueAtPercentile(99);
        result.aggregatedPublishDelayLatency999pct =
                aggregated.publishDelayLatency.getValueAtPercentile(99.9);
        result.aggregatedPublishDelayLatency9999pct =
                aggregated.publishDelayLatency.getValueAtPercentile(99.99);
        result.aggregatedPublishDelayLatencyMax = aggregated.publishDelayLatency.getMaxValue();

        result.aggregatedEndToEndLatencyAvg = aggregated.endToEndLatency.getMean() / 1000.0;
        result.aggregatedEndToEndLatency50pct =
                aggregated.endToEndLatency.getValueAtPercentile(50) / 1000.0;
        result.aggregatedEndToEndLatency75pct =
                aggregated.endToEndLatency.getValueAtPercentile(75) / 1000.0;
        result.aggregatedEndToEndLatency95pct =
                aggregated.endToEndLatency.getValueAtPercentile(95) / 1000.0;
        result.aggregatedEndToEndLatency99pct =
                aggregated.endToEndLatency.getValueAtPercentile(99) / 1000.0;
        result.aggregatedEndToEndLatency999pct =
                aggregated.endToEndLatency.getValueAtPercentile(99.9) / 1000.0;
        result.aggregatedEndToEndLatency9999pct =
                aggregated.endToEndLatency.getValueAtPercentile(99.99) / 1000.0;
        result.aggregatedEndToEndLatencyMax = aggregated.endToEndLatency.getMaxValue() / 1000.0;

        aggregated
                .publishLatency
                .percentiles(100)
                .forEach(
                        value ->
                                result.aggregatedPublishLatencyQuantiles.put(
                                        value.getPercentile(), value.getValueIteratedTo() / 1000.0));
        aggregated
                .publishDelayLatency
                .percentiles(100)
                .forEach(
                        value ->
                                result.aggregatedPublishDelayLatencyQuantiles.put(
                                        value.getPercentile(), value.getValueIteratedTo()));
        aggregated
                .endToEndLatency
                .percentiles(100)
                .forEach(
                        value ->
                                result.aggregatedEndToEndLatencyQuantiles.put(
                                        value.getPercentile(), microsToMillis(value.getValueIteratedTo())));
    }

    private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.0", 7);
    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 4);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.0", 4);

    private static double microsToMillis(double timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static double microsToMillis(long timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);

    private long deriveSeed(String domain, long ordinal) {
        return run == null ? new Random().nextLong() : SeedDerivation.derive(run.seed, domain, ordinal);
    }

    private static void validateRunConfiguration(RunConfiguration run, Workload workload) {
        if (run == null) {
            return;
        }
        if (!run.isConfigured()) {
            throw new IllegalArgumentException(
                    "formal run requires campaignId, blockId, runId, stage, repetition and seed");
        }
        if (!run.stage.matches("[ABCDE]") || run.runId.matches(".*[^A-Za-z0-9._-].*")) {
            throw new IllegalArgumentException("invalid formal run stage or runId");
        }
        if (workload.warmupDrainTimeoutSeconds <= 0 || workload.measurementDrainTimeoutSeconds <= 0) {
            throw new IllegalArgumentException(
                    "formal runs require warmupDrainTimeoutSeconds and "
                            + "measurementDrainTimeoutSeconds > 0");
        }
    }

    private void validateRuntimeInfo(DriverRuntimeInfo actual) {
        if (run == null) {
            return;
        }
        if (actual == null || actual.namespace == null || actual.namespace.isEmpty()) {
            throw new IllegalStateException("driver did not return runtime namespace evidence");
        }
        if (actual.run == null
                || !run.runId.equals(actual.run.runId)
                || !run.campaignId.equals(actual.run.campaignId)
                || !run.blockId.equals(actual.run.blockId)
                || !run.stage.equals(actual.run.stage)
                || run.repetition != actual.run.repetition
                || !run.seed.equals(actual.run.seed)) {
            throw new IllegalStateException("driver runtime run identity does not match driver YAML");
        }
    }
}
