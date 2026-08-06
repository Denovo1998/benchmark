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
package io.openmessaging.benchmark.driver.pulsar;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.DriverRuntimeInfo;
import io.openmessaging.benchmark.driver.ProducerOptions;
import io.openmessaging.benchmark.driver.RunConfiguration;
import io.openmessaging.benchmark.driver.pulsar.config.PulsarClientConfig.PersistenceConfiguration;
import io.openmessaging.benchmark.driver.pulsar.config.PulsarConfig;
import io.openmessaging.benchmark.driver.pulsar.config.PulsarProducerConfig;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarBenchmarkDriver implements BenchmarkDriver {

    private static final String OXIA_ADMIN_POLICY_ALREADY_EXISTS =
            "key already exists: /admin/policies/";
    private static final String NEREUS_NAMESPACE_POLICY_VERSION_CHANGED =
            "NEREUS_NAMESPACE_POLICY_VERSION_CHANGED";
    private static final int TOPIC_NOT_READY_MAX_RETRIES = 20;
    private static final long TOPIC_NOT_READY_RETRY_DELAY_MILLIS = 250L;
    private static final ScheduledExecutorService TOPIC_RETRY_EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "pulsar-topic-readiness-retry");
                        thread.setDaemon(true);
                        return thread;
                    });

    private PulsarClient client;
    private PulsarAdmin adminClient;

    private PulsarConfig config;

    private String namespace;
    private ProducerBuilder<byte[]> producerBuilder;
    private DriverRuntimeInfo runtimeInfo = new DriverRuntimeInfo();

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        this.config = readConfig(configurationFile);
        log.info("Pulsar driver configuration: {}", writer.writeValueAsString(config));
        validateConfiguration(config);

        ClientBuilder clientBuilder =
                PulsarClient.builder()
                        .ioThreads(config.client.ioThreads)
                        .connectionsPerBroker(config.client.connectionsPerBroker)
                        .statsInterval(0, TimeUnit.SECONDS)
                        .serviceUrl(config.client.serviceUrl)
                        .maxConcurrentLookupRequests(config.client.maxConcurrentLookupRequests)
                        .maxLookupRequests(Integer.MAX_VALUE)
                        .memoryLimit(config.client.clientMemoryLimitMB, SizeUnit.MEGA_BYTES)
                        .operationTimeout(10, TimeUnit.MINUTES)
                        .listenerThreads(Runtime.getRuntime().availableProcessors());

        if (config.client.serviceUrl.startsWith("pulsar+ssl")) {
            clientBuilder
                    .allowTlsInsecureConnection(config.client.tlsAllowInsecureConnection)
                    .enableTlsHostnameVerification(config.client.tlsEnableHostnameVerification)
                    .tlsTrustCertsFilePath(config.client.tlsTrustCertsFilePath);
        }

        PulsarAdminBuilder pulsarAdminBuilder =
                PulsarAdmin.builder().serviceHttpUrl(config.client.httpUrl);
        if (config.client.httpUrl.startsWith("https")) {
            pulsarAdminBuilder
                    .allowTlsInsecureConnection(config.client.tlsAllowInsecureConnection)
                    .enableTlsHostnameVerification(config.client.tlsEnableHostnameVerification)
                    .tlsTrustCertsFilePath(config.client.tlsTrustCertsFilePath);
        }

        if (config.client.authentication.plugin != null
                && !config.client.authentication.plugin.isEmpty()) {
            clientBuilder.authentication(
                    config.client.authentication.plugin, config.client.authentication.data);
            pulsarAdminBuilder.authentication(
                    config.client.authentication.plugin, config.client.authentication.data);
        }

        client = clientBuilder.build();

        log.info("Created Pulsar client for service URL {}", config.client.serviceUrl);

        adminClient = pulsarAdminBuilder.build();

        log.info("Created Pulsar admin client for HTTP URL {}", config.client.httpUrl);

        producerBuilder =
                client
                        .newProducer()
                        .enableBatching(config.producer.batchingEnabled)
                        .batchingMaxPublishDelay(
                                config.producer.batchingMaxPublishDelayMs, TimeUnit.MILLISECONDS)
                        .batchingMaxMessages(Integer.MAX_VALUE)
                        .batchingMaxBytes(config.producer.batchingMaxBytes)
                        .compressionType(config.producer.compressionType)
                        .compressionMinMsgBodySize(config.producer.compressionMinMsgBodySize)
                        .blockIfQueueFull(config.producer.blockIfQueueFull)
                        .sendTimeout(0, TimeUnit.MILLISECONDS)
                        .maxPendingMessages(config.producer.pendingQueueSize);

        try {
            // Create namespace and set the configuration
            String tenant = config.client.namespacePrefix.split("/")[0];
            String cluster = config.client.clusterName;
            if (!adminClient.tenants().getTenants().contains(tenant)) {
                try {
                    adminClient
                            .tenants()
                            .createTenant(
                                    tenant,
                                    TenantInfo.builder()
                                            .adminRoles(Collections.emptySet())
                                            .allowedClusters(Sets.newHashSet(cluster))
                                            .build());
                } catch (PulsarAdminException e) {
                    if (!isConcurrentAdminCreateConflict(e)) {
                        throw e;
                    }
                    // Multiple workers may race while creating the shared tenant.
                    log.info("Pulsar tenant {} was created concurrently; continuing", tenant);
                }
            }
            log.info("Created Pulsar tenant {} with allowed cluster {}", tenant, cluster);

            this.namespace = buildNamespace(config);
            initializeNamespaceIdentity(namespace, config.run);
            log.info("Created or verified Pulsar namespace {}", namespace);

            PersistenceConfiguration p = config.client.persistence;
            PersistencePolicies expected =
                    new PersistencePolicies(
                            p.ensembleSize, p.writeQuorum, p.ackQuorum, 1.0, p.managedLedgerStorageClassName);
            applyAndVerifyPersistence(expected);

            adminClient
                    .namespaces()
                    .setBacklogQuota(
                            namespace,
                            BacklogQuota.builder()
                                    .limitSize(-1L)
                                    .limitTime(-1)
                                    .retentionPolicy(RetentionPolicy.producer_exception)
                                    .build());
            adminClient.namespaces().setDeduplicationStatus(namespace, p.deduplicationEnabled);
            Boolean actualDeduplication = adminClient.namespaces().getDeduplicationStatus(namespace);
            if (!Objects.equals(Boolean.valueOf(p.deduplicationEnabled), actualDeduplication)) {
                throw new IOException(
                        "Namespace "
                                + namespace
                                + " deduplication mismatch: expected="
                                + p.deduplicationEnabled
                                + ", actual="
                                + actualDeduplication);
            }
            runtimeInfo = buildRuntimeInfo(expected, actualDeduplication);
            log.info(
                    "Applied persistence configuration for namespace {}/{}/{}: {}",
                    tenant,
                    cluster,
                    namespace,
                    writer.writeValueAsString(p));

        } catch (PulsarAdminException e) {
            throw new IOException(e);
        }
    }

    @Override
    public DriverRuntimeInfo getRuntimeInfo() {
        return runtimeInfo;
    }

    @Override
    public CompletableFuture<Long> getSubscriptionBacklog(String topic, String subscriptionName) {
        try {
            PartitionedTopicMetadata metadata = adminClient.topics().getPartitionedTopicMetadata(topic);
            if (metadata.partitions > 0) {
                PartitionedTopicStats stats = adminClient.topics().getPartitionedStats(topic, true);
                if (stats.getPartitions() == null || stats.getPartitions().isEmpty()) {
                    throw new IllegalStateException("partitioned topic has no partition stats: " + topic);
                }
                long backlog = 0;
                for (TopicStats partitionStats : stats.getPartitions().values()) {
                    backlog += subscriptionBacklog(partitionStats, topic, subscriptionName);
                }
                return CompletableFuture.completedFuture(backlog);
            }
            return CompletableFuture.completedFuture(
                    subscriptionBacklog(adminClient.topics().getStats(topic), topic, subscriptionName));
        } catch (PulsarAdminException | RuntimeException e) {
            CompletableFuture<Long> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    private static long subscriptionBacklog(
            TopicStats topicStats, String topic, String subscriptionName) {
        Map<String, ? extends SubscriptionStats> subscriptions = topicStats.getSubscriptions();
        SubscriptionStats subscription =
                subscriptions == null ? null : subscriptions.get(subscriptionName);
        if (subscription == null) {
            throw new IllegalStateException(
                    "subscription " + subscriptionName + " is not present in topic stats " + topic);
        }
        return subscription.getMsgBacklog();
    }

    @Override
    public String getTopicNamePrefix() {
        return config.client.topicType + "://" + namespace + "/test";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        if (partitions == 1) {
            return adminClient.topics().createNonPartitionedTopicAsync(topic);
        }

        return adminClient.topics().createPartitionedTopicAsync(topic, partitions);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(
            String topic, ProducerOptions options) {
        ProducerOptions effectiveOptions = getEffectiveProducerOptions(options, config.producer);
        validateDelayedDeliveryConfiguration(effectiveOptions, config.consumer.subscriptionType);

        return producerBuilder
                .topic(topic)
                .createAsync()
                .thenApply(p -> new PulsarBenchmarkProducer(p, effectiveOptions));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        List<CompletableFuture<Consumer<ByteBuffer>>> futures = new ArrayList<>();
        return client
                .getPartitionsForTopic(topic)
                .thenCompose(
                        partitions -> {
                            partitions.forEach(
                                    p -> futures.add(createInternalConsumer(p, subscriptionName, consumerCallback)));
                            return FutureUtil.waitForAll(futures);
                        })
                .thenApply(
                        __ ->
                                new PulsarBenchmarkConsumer(
                                        futures.stream().map(CompletableFuture::join).collect(Collectors.toList())));
    }

    CompletableFuture<Consumer<ByteBuffer>> createInternalConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        return createInternalConsumer(topic, subscriptionName, consumerCallback, 0);
    }

    private CompletableFuture<Consumer<ByteBuffer>> createInternalConsumer(
            String topic,
            String subscriptionName,
            ConsumerCallback consumerCallback,
            int attempt) {
        return client
                .newConsumer(Schema.BYTEBUFFER)
                .priorityLevel(0)
                .subscriptionType(config.consumer.subscriptionType)
                .messageListener(
                        (c, msg) -> {
                            try {
                                consumerCallback.messageReceived(msg.getValue(), msg.getPublishTime());
                                c.acknowledgeAsync(msg);
                            } finally {
                                msg.release();
                            }
                        })
                .topic(topic)
                .subscriptionName(subscriptionName)
                .receiverQueueSize(config.consumer.receiverQueueSize)
                .maxTotalReceiverQueueSizeAcrossPartitions(
                        config.consumer.maxTotalReceiverQueueSizeAcrossPartitions)
                .poolMessages(true)
                .subscribeAsync()
                .handle(
                        (consumer, error) -> {
                            if (error == null) {
                                return CompletableFuture.completedFuture(consumer);
                            }

                            Throwable cause = FutureUtil.unwrapCompletionException(error);
                            if (!isTopicNotReady(cause) || attempt >= TOPIC_NOT_READY_MAX_RETRIES) {
                                CompletableFuture<Consumer<ByteBuffer>> failed = new CompletableFuture<>();
                                failed.completeExceptionally(cause);
                                return failed;
                            }

                            long delayMillis =
                                    Math.min(
                                            1000L,
                                            TOPIC_NOT_READY_RETRY_DELAY_MILLIS
                                                    << Math.min(attempt, 2));
                            log.info(
                                    "Pulsar topic {} is not ready; retrying consumer creation "
                                            + "{}/{} after {} ms",
                                    topic,
                                    attempt + 1,
                                    TOPIC_NOT_READY_MAX_RETRIES,
                                    delayMillis);
                            return delayBeforeTopicRetry(delayMillis)
                                    .thenCompose(
                                            ignored ->
                                                    createInternalConsumer(
                                                            topic,
                                                            subscriptionName,
                                                            consumerCallback,
                                                            attempt + 1));
                        })
                .thenCompose(Function.identity());
    }

    private static CompletableFuture<Void> delayBeforeTopicRetry(long delayMillis) {
        CompletableFuture<Void> delay = new CompletableFuture<>();
        TOPIC_RETRY_EXECUTOR.schedule(
                () -> delay.complete(null), delayMillis, TimeUnit.MILLISECONDS);
        return delay;
    }

    static boolean isTopicNotReady(Throwable error) {
        return error instanceof PulsarClientException.TopicDoesNotExistException;
    }

    @Override
    public void close() throws Exception {
        log.info("Shutting down Pulsar benchmark driver");

        if (client != null) {
            client.close();
        }

        if (adminClient != null) {
            adminClient.close();
        }

        log.info("Pulsar benchmark driver successfully shut down");
    }

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static ProducerOptions getEffectiveProducerOptions(
            ProducerOptions options, PulsarProducerConfig producerConfig) {
        ProducerOptions effectiveOptions = new ProducerOptions();
        effectiveOptions.messageDelayMs =
                options.messageDelayMs > 0 ? options.messageDelayMs : producerConfig.messageDelayMs;
        effectiveOptions.delayMessageRatio =
                options.delayMessageRatio > 0.0
                        ? options.delayMessageRatio
                        : producerConfig.delayMessageRatio;
        effectiveOptions.minMessageDelayMs =
                options.minMessageDelayMs > 0
                        ? options.minMessageDelayMs
                        : producerConfig.minMessageDelayMs;
        effectiveOptions.maxMessageDelayMs =
                options.maxMessageDelayMs > 0
                        ? options.maxMessageDelayMs
                        : producerConfig.maxMessageDelayMs;
        return effectiveOptions;
    }

    static void validateDelayedDeliveryConfiguration(
            ProducerOptions options, SubscriptionType subscriptionType) {
        if (options.delayMessageRatio <= 0.0d) {
            return;
        }

        if (options.messageDelayMs <= 0 && options.maxMessageDelayMs <= 0) {
            throw new IllegalArgumentException(
                    "Delayed delivery is enabled via delayMessageRatio > 0, but neither"
                            + " messageDelayMs nor maxMessageDelayMs is set to a positive value.");
        }

        if (subscriptionType == SubscriptionType.Shared
                || subscriptionType == SubscriptionType.Key_Shared) {
            return;
        }

        throw new IllegalArgumentException(
                "Pulsar delayed delivery requires consumer.subscriptionType to be Shared or"
                        + " Key_Shared, but found "
                        + subscriptionType
                        + ". Pulsar dispatches delayed messages immediately for this"
                        + " subscription type.");
    }

    private static PulsarConfig readConfig(File configurationFile) throws IOException {
        return mapper.readValue(configurationFile, PulsarConfig.class);
    }

    static void validateConfiguration(PulsarConfig config) {
        if (config == null
                || config.client == null
                || config.client.namespacePrefix == null
                || config.client.namespacePrefix.trim().isEmpty()) {
            throw new IllegalArgumentException("client.namespacePrefix must be tenant/namespace");
        }
        if (config.client.persistence == null || config.producer == null || config.consumer == null) {
            throw new IllegalArgumentException("client.persistence, producer and consumer are required");
        }
        String[] namespaceParts = config.client.namespacePrefix.split("/", -1);
        if (namespaceParts.length != 2
                || namespaceParts[0].trim().isEmpty()
                || namespaceParts[1].trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "client.namespacePrefix must contain exactly two non-empty components: tenant/namespace");
        }
        if (config.client.namespaceSuffix != null && !isSafeRunId(config.client.namespaceSuffix)) {
            throw new IllegalArgumentException("client.namespaceSuffix contains unsafe characters");
        }
        RunConfiguration run = config.run;
        if (run != null) {
            if (!run.isConfigured()) {
                throw new IllegalArgumentException(
                        "run requires campaignId, blockId, runId, stage, repetition and seed");
            }
            if (!isSafeRunId(run.runId) || run.runId.length() > 128) {
                throw new IllegalArgumentException(
                        "run.runId must be a safe value of at most 128 characters");
            }
            if (config.client.namespaceSuffix == null
                    || !run.runId.equals(config.client.namespaceSuffix)) {
                throw new IllegalArgumentException("run.runId must equal client.namespaceSuffix");
            }
            String stage = run.stage.toUpperCase();
            if (!stage.matches("[ABCDE]")) {
                throw new IllegalArgumentException("run.stage must be one of A, B, C, D or E");
            }
            String storageClass = config.client.persistence.managedLedgerStorageClassName;
            boolean bookkeeper = "A".equals(stage) || "B".equals(stage);
            String expected = bookkeeper ? "bookkeeper" : "nereus";
            if (!expected.equals(storageClass)) {
                throw new IllegalArgumentException(
                        "stage " + stage + " requires managedLedgerStorageClassName=" + expected);
            }
        }
    }

    private static boolean isSafeRunId(String value) {
        return value != null && value.matches("[A-Za-z0-9._-]+");
    }

    private static String buildNamespace(PulsarConfig config) {
        String suffix = config.client.namespaceSuffix;
        return config.client.namespacePrefix
                + "-"
                + (suffix == null || suffix.isEmpty() ? getRandomString() : suffix);
    }

    private void initializeNamespaceIdentity(String namespace, RunConfiguration run)
            throws PulsarAdminException, IOException {
        if (run == null) {
            adminClient.namespaces().createNamespace(namespace);
            return;
        }

        boolean created = false;
        try {
            adminClient.namespaces().createNamespace(namespace);
            created = true;
        } catch (PulsarAdminException e) {
            if (!isConcurrentAdminCreateConflict(e)) {
                throw e;
            }
            log.info("Namespace {} already exists; verifying run identity", namespace);
        }

        Map<String, String> expected = namespaceProperties(run);
        if (created) {
            adminClient.namespaces().setProperties(namespace, expected);
            Map<String, String> actual = adminClient.namespaces().getProperties(namespace);
            if (!expected.equals(actual)) {
                throw new IOException(
                        "Namespace "
                                + namespace
                                + " properties mismatch: expected="
                                + expected
                                + ", actual="
                                + actual);
            }
            return;
        }

        PulsarAdminException last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                Map<String, String> actual = adminClient.namespaces().getProperties(namespace);
                if (expected.equals(actual)) {
                    return;
                }
                last =
                        new PulsarAdminException(
                                "Namespace "
                                        + namespace
                                        + " has run properties "
                                        + actual
                                        + ", expected "
                                        + expected);
            } catch (PulsarAdminException e) {
                last = e;
            }
            sleepBeforeRetry(attempt);
        }
        throw new IOException("Namespace run identity did not converge for " + namespace, last);
    }

    private static Map<String, String> namespaceProperties(RunConfiguration run) {
        Map<String, String> properties = new TreeMap<>();
        properties.put("omb.schema-version", "1");
        properties.put("omb.campaign-id", run.campaignId);
        properties.put("omb.block-id", run.blockId);
        properties.put("omb.run-id", run.runId);
        properties.put("omb.stage", run.stage);
        properties.put("omb.repetition", Integer.toString(run.repetition));
        properties.put("omb.seed", Long.toString(run.seed));
        return properties;
    }

    private void applyAndVerifyPersistence(PersistencePolicies expected)
            throws PulsarAdminException, IOException {
        PersistencePolicies actual = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                adminClient.namespaces().setPersistence(namespace, expected);
                actual = adminClient.namespaces().getPersistence(namespace);
                if (verifyPersistencePolicy(expected, actual)) {
                    return;
                }
            } catch (PulsarAdminException e) {
                if (!isTransientPersistenceConflict(e)) {
                    throw e;
                }
                log.info("Persistence policy update conflict for {}, retry {}", namespace, attempt + 1);
            }
            sleepBeforeRetry(attempt);
        }
        throw new IOException(
                "Persistence policy mismatch for namespace "
                        + namespace
                        + ": expected="
                        + expected
                        + ", actual="
                        + actual);
    }

    static boolean isTransientPersistenceConflict(PulsarAdminException exception) {
        if (exception instanceof ConflictException) {
            return true;
        }
        if (exception.getStatusCode() < 500 || exception.getStatusCode() >= 600) {
            return false;
        }
        return containsNereusPolicyVersionMarker(exception.getHttpError())
                || containsNereusPolicyVersionMarker(exception.getMessage());
    }

    static boolean isConcurrentAdminCreateConflict(PulsarAdminException exception) {
        if (exception instanceof ConflictException) {
            return true;
        }
        if (exception.getStatusCode() < 500 || exception.getStatusCode() >= 600) {
            return false;
        }
        return containsOxiaAdminPolicyAlreadyExistsMarker(exception.getHttpError())
                || containsOxiaAdminPolicyAlreadyExistsMarker(exception.getMessage());
    }

    private static boolean containsOxiaAdminPolicyAlreadyExistsMarker(String value) {
        return value != null && value.contains(OXIA_ADMIN_POLICY_ALREADY_EXISTS);
    }

    private static boolean containsNereusPolicyVersionMarker(String value) {
        return value != null && value.contains(NEREUS_NAMESPACE_POLICY_VERSION_CHANGED);
    }

    static boolean verifyPersistencePolicy(PersistencePolicies expected, PersistencePolicies actual) {
        return actual != null
                && expected.getBookkeeperEnsemble() == actual.getBookkeeperEnsemble()
                && expected.getBookkeeperWriteQuorum() == actual.getBookkeeperWriteQuorum()
                && expected.getBookkeeperAckQuorum() == actual.getBookkeeperAckQuorum()
                && Double.compare(
                                expected.getManagedLedgerMaxMarkDeleteRate(),
                                actual.getManagedLedgerMaxMarkDeleteRate())
                        == 0
                && Objects.equals(
                        expected.getManagedLedgerStorageClassName(), actual.getManagedLedgerStorageClassName());
    }

    private DriverRuntimeInfo buildRuntimeInfo(PersistencePolicies policy, Boolean deduplication) {
        DriverRuntimeInfo info = new DriverRuntimeInfo();
        info.run = config.run;
        info.namespace = namespace;
        info.attributes.put(
                "persistence.bookkeeperEnsemble", Integer.toString(policy.getBookkeeperEnsemble()));
        info.attributes.put(
                "persistence.bookkeeperWriteQuorum", Integer.toString(policy.getBookkeeperWriteQuorum()));
        info.attributes.put(
                "persistence.bookkeeperAckQuorum", Integer.toString(policy.getBookkeeperAckQuorum()));
        info.attributes.put(
                "persistence.managedLedgerMaxMarkDeleteRate",
                Double.toString(policy.getManagedLedgerMaxMarkDeleteRate()));
        info.attributes.put(
                "persistence.managedLedgerStorageClassName",
                String.valueOf(policy.getManagedLedgerStorageClassName()));
        info.attributes.put(
                "persistence.expectedBookkeeperEnsemble",
                Integer.toString(config.client.persistence.ensembleSize));
        info.attributes.put(
                "persistence.expectedBookkeeperWriteQuorum",
                Integer.toString(config.client.persistence.writeQuorum));
        info.attributes.put(
                "persistence.expectedBookkeeperAckQuorum",
                Integer.toString(config.client.persistence.ackQuorum));
        info.attributes.put(
                "persistence.expectedManagedLedgerStorageClassName",
                String.valueOf(config.client.persistence.managedLedgerStorageClassName));
        info.attributes.put("persistence.expectedManagedLedgerMaxMarkDeleteRate", "1.0");
        info.attributes.put("namespace.deduplicationEnabled", String.valueOf(deduplication));
        info.attributes.put(
                "producer.compressionType", String.valueOf(config.producer.compressionType));
        info.attributes.put(
                "producer.compressionMinMsgBodySize",
                Integer.toString(config.producer.compressionMinMsgBodySize));
        info.attributes.put(
                "producer.batchingEnabled", Boolean.toString(config.producer.batchingEnabled));
        info.attributes.put(
                "producer.batchingMaxPublishDelayMs",
                Integer.toString(config.producer.batchingMaxPublishDelayMs));
        info.attributes.put(
                "producer.batchingMaxBytes", Integer.toString(config.producer.batchingMaxBytes));
        info.attributes.put(
                "consumer.subscriptionType", String.valueOf(config.consumer.subscriptionType));
        info.attributes.put(
                "consumer.receiverQueueSize", Integer.toString(config.consumer.receiverQueueSize));
        info.attributes.put(
                "consumer.maxTotalReceiverQueueSizeAcrossPartitions",
                Integer.toString(config.consumer.maxTotalReceiverQueueSizeAcrossPartitions));
        return info;
    }

    private static void sleepBeforeRetry(int attempt) throws IOException {
        try {
            Thread.sleep(Math.min(1000L, 100L << Math.min(attempt, 3)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for Pulsar metadata to converge", e);
        }
    }

    private static final Random random = new Random();

    private static String getRandomString() {
        byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return BaseEncoding.base64Url().omitPadding().encode(buffer);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static final Logger log = LoggerFactory.getLogger(PulsarBenchmarkDriver.class);
}
