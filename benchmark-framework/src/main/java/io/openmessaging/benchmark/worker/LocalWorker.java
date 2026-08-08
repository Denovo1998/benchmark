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

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ConsumerInfo;
import io.openmessaging.benchmark.driver.BenchmarkDriver.ProducerInfo;
import io.openmessaging.benchmark.driver.BenchmarkDriver.TopicInfo;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.DriverRuntimeInfo;
import io.openmessaging.benchmark.driver.ProducerOptions;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.SeedDerivation;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.utils.payload.PayloadPoolFactory;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalWorker implements Worker, ConsumerCallback {

    private BenchmarkDriver benchmarkDriver = null;
    private final List<BenchmarkProducer> producers = new ArrayList<>();
    private final List<BenchmarkConsumer> consumers = new ArrayList<>();
    private volatile MessageProducer messageProducer;
    private volatile double currentPublishRate = 1.0;
    private volatile ProducerGate producerGate = new ProducerGate(0);
    private final ExecutorService executor =
            Executors.newCachedThreadPool(new DefaultThreadFactory("local-worker"));
    private final StatsLogger statsLogger;
    private volatile WorkerStats stats;
    private final AtomicLong loadGeneration = new AtomicLong();
    private Long topicNameSeed;

    public LocalWorker() {
        this(NullStatsLogger.INSTANCE);
    }

    public LocalWorker(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        stats = new WorkerStats(statsLogger);
        updateMessageProducer(1.0);
    }

    @Override
    public void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        loadGeneration.incrementAndGet();
        stats = new WorkerStats(statsLogger);
        currentPublishRate = 1.0;
        updateMessageProducer(currentPublishRate);

        DriverConfiguration driverConfiguration =
                mapper.readValue(driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver =
                    (BenchmarkDriver) Class.forName(driverConfiguration.driverClass).newInstance();
            benchmarkDriver.initialize(driverConfigFile, stats.getStatsLogger());
        } catch (InstantiationException
                | IllegalAccessException
                | ClassNotFoundException
                | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        Timer timer = new Timer();
        topicNameSeed = topicsInfo.topicNameSeed;

        List<TopicInfo> topicInfos =
                IntStream.range(0, topicsInfo.numberOfTopics)
                        .mapToObj(
                                i -> new TopicInfo(generateTopicName(i), topicsInfo.numberOfPartitionsPerTopic))
                        .collect(toList());

        benchmarkDriver.createTopics(topicInfos).join();

        List<String> topics = topicInfos.stream().map(TopicInfo::getTopic).collect(toList());

        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());
        return topics;
    }

    @Override
    public DriverRuntimeInfo getDriverRuntimeInfo() throws IOException {
        if (benchmarkDriver == null) {
            throw new IOException("driver is not initialized");
        }
        return benchmarkDriver.getRuntimeInfo();
    }

    @Override
    public long getSubscriptionBacklog(String topic, String subscriptionName) throws IOException {
        if (benchmarkDriver == null) {
            throw new IOException("driver is not initialized");
        }
        try {
            return benchmarkDriver
                    .getSubscriptionBacklog(topic, subscriptionName)
                    .toCompletableFuture()
                    .join();
        } catch (RuntimeException e) {
            throw new IOException("failed to read broker subscription backlog", e);
        }
    }

    private String generateTopicName(int i) {
        String suffix =
                topicNameSeed == null
                        ? RandomGenerator.getRandomString()
                        : String.format("%016x", SeedDerivation.derive(topicNameSeed, "topic-name", i));
        return String.format("%s-%07d-%s", benchmarkDriver.getTopicNamePrefix(), i, suffix);
    }

    @Override
    public void createProducers(List<String> topics, Workload workload) {
        Timer timer = new Timer();
        AtomicInteger index = new AtomicInteger();

        ProducerOptions options = new ProducerOptions();
        options.messageDelayMs = workload.messageDelayMs;
        options.minMessageDelayMs = workload.minMessageDelayMs;
        options.maxMessageDelayMs = workload.maxMessageDelayMs;
        options.delayMessageRatio = workload.delayMessageRatio;

        producers.addAll(
                benchmarkDriver
                        .createProducers(
                                topics.stream()
                                        .map(t -> new ProducerInfo(index.getAndIncrement(), t))
                                        .collect(toList()),
                                options)
                        .join());

        log.info("Created {} producers in {} ms", producers.size(), timer.elapsedMillis());
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        Timer timer = new Timer();
        AtomicInteger index = new AtomicInteger();
        ConsumerCallback callback = new StatsConsumerCallback(stats);

        List<ConsumerInfo> consumerInfos =
                consumerAssignment.topicsSubscriptions.stream()
                        .map(c -> new ConsumerInfo(index.getAndIncrement(), c.topic, c.subscription, callback))
                        .collect(toList());
        consumers.addAll(benchmarkDriver.createConsumers(consumerInfos).join());

        log.info("Created {} consumers in {} ms", consumers.size(), timer.elapsedMillis());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        long generation = loadGeneration.incrementAndGet();
        int processors = Runtime.getRuntime().availableProcessors();
        List<byte[]> payloads = resolvePayloads(producerWorkAssignment);

        updateMessageProducer(producerWorkAssignment.publishRate);

        Map<Integer, List<BenchmarkProducer>> processorAssignment = new TreeMap<>();

        int processorIdx = 0;
        for (BenchmarkProducer p : producers) {
            processorAssignment
                    .computeIfAbsent(processorIdx, x -> new ArrayList<BenchmarkProducer>())
                    .add(p);

            processorIdx = (processorIdx + 1) % processors;
        }

        ProducerGate gate = new ProducerGate(processorAssignment.size());
        producerGate = gate;

        int executorOrdinal = 0;
        for (List<BenchmarkProducer> assignedProducers : processorAssignment.values()) {
            submitProducersToExecutor(
                    assignedProducers,
                    producerWorkAssignment.keyDistributorType,
                    payloads,
                    producerWorkAssignment.payloadSelectionSeed,
                    executorOrdinal++,
                    gate,
                    generation);
        }
    }

    private List<byte[]> resolvePayloads(ProducerWorkAssignment assignment) {
        if (assignment.payloadSpec != null && assignment.payloadData != null) {
            throw new IllegalArgumentException("payloadSpec and legacy payloadData cannot both be set");
        }
        if (assignment.payloadSpec != null) {
            return PayloadPoolFactory.create(assignment.payloadSpec);
        }
        if (assignment.payloadData == null || assignment.payloadData.isEmpty()) {
            throw new IllegalArgumentException("producer payload assignment is empty");
        }
        return assignment.payloadData;
    }

    @Override
    public void probeProducers() throws IOException {
        List<CompletableFuture<Void>> probes = new ArrayList<>(producers.size());
        for (BenchmarkProducer producer : producers) {
            stats.recordProducerStarted();
            CompletableFuture<Void> probe;
            try {
                probe = producer.sendAsync(Optional.of("key"), new byte[10]);
            } catch (Throwable error) {
                probe = new CompletableFuture<>();
                probe.completeExceptionally(error);
            }
            probes.add(
                    probe.whenComplete(
                            (ignored, error) -> {
                                if (error == null) {
                                    stats.recordProbeSuccess();
                                } else {
                                    stats.recordProducerFailure();
                                }
                            }));
        }

        try {
            CompletableFuture.allOf(probes.toArray(new CompletableFuture<?>[0])).join();
        } catch (CompletionException error) {
            throw new IOException("readiness probe send failed", unwrapCompletionException(error));
        }
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private void submitProducersToExecutor(
            List<BenchmarkProducer> producers,
            io.openmessaging.benchmark.utils.distributor.KeyDistributorType keyDistributorType,
            List<byte[]> payloads,
            long payloadSelectionSeed,
            int executorOrdinal,
            ProducerGate gate,
            long generation) {
        java.util.Random r =
                new java.util.Random(
                        SeedDerivation.derive(
                                payloadSelectionSeed, "executor-payload-selection", executorOrdinal));
        KeyDistributor keyDistributor =
                KeyDistributor.build(
                        keyDistributorType,
                        SeedDerivation.derive(payloadSelectionSeed, "key-selection", executorOrdinal));
        int payloadCount = payloads.size();
        executor.submit(
                () -> {
                    try {
                        while (loadGeneration.get() == generation) {
                            for (BenchmarkProducer producer : producers) {
                                gate.enter();
                                try {
                                    if (loadGeneration.get() != generation) {
                                        return;
                                    }
                                    messageProducer.sendMessage(
                                            producer,
                                            Optional.ofNullable(keyDistributor.next()),
                                            payloads.get(r.nextInt(payloadCount)),
                                            () -> loadGeneration.get() != generation);
                                } finally {
                                    gate.exit();
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log.error("Got error", t);
                    }
                });
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        if (publishRate < 1.0) {
            updateMessageProducer(1.0);
            return;
        }
        updateMessageProducer(publishRate);
    }

    @Override
    public void pauseProducers() {
        producerGate.pause();
        log.info("Paused producers");
    }

    @Override
    public void resumeProducers() {
        updateMessageProducer(currentPublishRate);
        producerGate.resume();
        log.info("Resumed producers at {} msg/s", currentPublishRate);
    }

    private void updateMessageProducer(double publishRate) {
        currentPublishRate = publishRate;
        messageProducer = new MessageProducer(new UniformRateLimiter(publishRate), stats);
    }

    @Override
    public PeriodStats getPeriodStats() {
        return stats.toPeriodStats();
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return stats.toCumulativeLatencies();
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        return stats.toCountersStats();
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        internalMessageReceived(data.length, publishTimestamp);
    }

    @Override
    public void messageReceived(ByteBuffer data, long publishTimestamp) {
        internalMessageReceived(data.remaining(), publishTimestamp);
    }

    @Override
    public void messageAcknowledgementStarted() {
        stats.recordAcknowledgementStarted();
    }

    @Override
    public void messageAcknowledgementCompleted(Throwable error) {
        stats.recordAcknowledgementCompleted(error);
    }

    public void internalMessageReceived(int size, long publishTimestamp) {
        recordMessageReceived(stats, size, publishTimestamp);
    }

    private static void recordMessageReceived(WorkerStats stats, int size, long publishTimestamp) {
        long now = System.currentTimeMillis();
        long endToEndLatencyMicros = TimeUnit.MILLISECONDS.toMicros(now - publishTimestamp);
        stats.recordMessageReceived(size, endToEndLatencyMicros);
    }

    @Override
    public void pauseConsumers() throws IOException {
        for (BenchmarkConsumer consumer : consumers) {
            consumer.pause();
        }
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        for (BenchmarkConsumer consumer : consumers) {
            consumer.resume();
        }
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() throws IOException {
        stats.resetMeasurement();
    }

    @Override
    public void stopAll() {
        loadGeneration.incrementAndGet();
        producerGate.resume();

        try {
            for (BenchmarkProducer producer : producers) {
                producer.close();
            }
            producers.clear();

            for (BenchmarkConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();

            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            stats.reset();
            producerGate = new ProducerGate(0);
        }
    }

    @Override
    public String id() {
        return "local";
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final class ProducerGate {
        private final Semaphore permits;
        private final int permitCount;
        private boolean paused;

        private ProducerGate(int permitCount) {
            this.permitCount = permitCount;
            permits = new Semaphore(permitCount, true);
        }

        private void enter() {
            permits.acquireUninterruptibly();
        }

        private void exit() {
            permits.release();
        }

        private synchronized void pause() {
            if (paused) {
                return;
            }
            permits.acquireUninterruptibly(permitCount);
            paused = true;
        }

        private synchronized void resume() {
            if (!paused) {
                return;
            }
            paused = false;
            permits.release(permitCount);
        }
    }

    private static final class StatsConsumerCallback implements ConsumerCallback {
        private final WorkerStats stats;

        private StatsConsumerCallback(WorkerStats stats) {
            this.stats = stats;
        }

        @Override
        public void messageReceived(byte[] data, long publishTimestamp) {
            recordMessageReceived(stats, data.length, publishTimestamp);
        }

        @Override
        public void messageReceived(ByteBuffer data, long publishTimestamp) {
            recordMessageReceived(stats, data.remaining(), publishTimestamp);
        }

        @Override
        public void messageAcknowledgementStarted() {
            stats.recordAcknowledgementStarted();
        }

        @Override
        public void messageAcknowledgementCompleted(Throwable error) {
            stats.recordAcknowledgementCompleted(error);
        }
    }

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}
