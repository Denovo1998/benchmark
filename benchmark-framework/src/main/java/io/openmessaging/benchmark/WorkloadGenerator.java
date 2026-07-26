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

        worker.resetStats();
        log.info("----- Starting benchmark traffic ({}m)------", workload.testDurationMinutes);

        TestResult result = printAndCollectStats(workload.testDurationMinutes, TimeUnit.MINUTES);
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

    private void ensureTopicsAreReady() throws IOException {
        log.info("Waiting for consumers to be ready");
        // This is work around the fact that there's no way to have a consumer ready in Kafka without
        // first publishing
        // some message on the topic, which will then trigger the partitions assignment to the consumers

        int expectedMessages = workload.topics * workload.subscriptionsPerTopic;

        // In this case we just publish 1 message and then wait for consumers to receive the data
        worker.probeProducers();

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
            throw new RuntimeException("Timed out waiting for consumers to be ready");
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
                            "driver does not expose broker backlog for formal B1 run: "
                                    + target.topic
                                    + "/"
                                    + target.subscription);
                }
                return null;
            }
            total += backlog;
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
                CumulativeLatencies agg = worker.getCumulativeLatencies();
                log.info(
                        "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {} | Pub Delay (us)  avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}",
                        dec.format(agg.publishLatency.getMean() / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(50) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(95) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99.9) / 1000.0),
                        dec.format(agg.publishLatency.getValueAtPercentile(99.99) / 1000.0),
                        throughputFormat.format(agg.publishLatency.getMaxValue() / 1000.0),
                        dec.format(agg.publishDelayLatency.getMean()),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(50)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(95)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(99)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(99.9)),
                        dec.format(agg.publishDelayLatency.getValueAtPercentile(99.99)),
                        throughputFormat.format(agg.publishDelayLatency.getMaxValue()));

                result.aggregatedPublishLatencyAvg = agg.publishLatency.getMean() / 1000.0;
                result.aggregatedPublishLatency50pct = agg.publishLatency.getValueAtPercentile(50) / 1000.0;
                result.aggregatedPublishLatency75pct = agg.publishLatency.getValueAtPercentile(75) / 1000.0;
                result.aggregatedPublishLatency95pct = agg.publishLatency.getValueAtPercentile(95) / 1000.0;
                result.aggregatedPublishLatency99pct = agg.publishLatency.getValueAtPercentile(99) / 1000.0;
                result.aggregatedPublishLatency999pct =
                        agg.publishLatency.getValueAtPercentile(99.9) / 1000.0;
                result.aggregatedPublishLatency9999pct =
                        agg.publishLatency.getValueAtPercentile(99.99) / 1000.0;
                result.aggregatedPublishLatencyMax = agg.publishLatency.getMaxValue() / 1000.0;

                result.aggregatedPublishDelayLatencyAvg = agg.publishDelayLatency.getMean();
                result.aggregatedPublishDelayLatency50pct =
                        agg.publishDelayLatency.getValueAtPercentile(50);
                result.aggregatedPublishDelayLatency75pct =
                        agg.publishDelayLatency.getValueAtPercentile(75);
                result.aggregatedPublishDelayLatency95pct =
                        agg.publishDelayLatency.getValueAtPercentile(95);
                result.aggregatedPublishDelayLatency99pct =
                        agg.publishDelayLatency.getValueAtPercentile(99);
                result.aggregatedPublishDelayLatency999pct =
                        agg.publishDelayLatency.getValueAtPercentile(99.9);
                result.aggregatedPublishDelayLatency9999pct =
                        agg.publishDelayLatency.getValueAtPercentile(99.99);
                result.aggregatedPublishDelayLatencyMax = agg.publishDelayLatency.getMaxValue();

                result.aggregatedEndToEndLatencyAvg = agg.endToEndLatency.getMean() / 1000.0;
                result.aggregatedEndToEndLatency50pct =
                        agg.endToEndLatency.getValueAtPercentile(50) / 1000.0;
                result.aggregatedEndToEndLatency75pct =
                        agg.endToEndLatency.getValueAtPercentile(75) / 1000.0;
                result.aggregatedEndToEndLatency95pct =
                        agg.endToEndLatency.getValueAtPercentile(95) / 1000.0;
                result.aggregatedEndToEndLatency99pct =
                        agg.endToEndLatency.getValueAtPercentile(99) / 1000.0;
                result.aggregatedEndToEndLatency999pct =
                        agg.endToEndLatency.getValueAtPercentile(99.9) / 1000.0;
                result.aggregatedEndToEndLatency9999pct =
                        agg.endToEndLatency.getValueAtPercentile(99.99) / 1000.0;
                result.aggregatedEndToEndLatencyMax = agg.endToEndLatency.getMaxValue() / 1000.0;

                agg.publishLatency
                        .percentiles(100)
                        .forEach(
                                value -> {
                                    result.aggregatedPublishLatencyQuantiles.put(
                                            value.getPercentile(), value.getValueIteratedTo() / 1000.0);
                                });

                agg.publishDelayLatency
                        .percentiles(100)
                        .forEach(
                                value -> {
                                    result.aggregatedPublishDelayLatencyQuantiles.put(
                                            value.getPercentile(), value.getValueIteratedTo());
                                });

                agg.endToEndLatency
                        .percentiles(100)
                        .forEach(
                                value -> {
                                    result.aggregatedEndToEndLatencyQuantiles.put(
                                            value.getPercentile(), microsToMillis(value.getValueIteratedTo()));
                                });

                break;
            }

            oldTime = now;
        }

        return result;
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
