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
package io.openmessaging.benchmark.driver;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Value;
import org.apache.bookkeeper.stats.StatsLogger;

/** Base driver interface. */
public interface BenchmarkDriver extends AutoCloseable {
    /**
     * Return the effective runtime configuration observed by the driver.
     *
     * @return the runtime configuration evidence
     */
    default DriverRuntimeInfo getRuntimeInfo() {
        return new DriverRuntimeInfo();
    }

    /**
     * Read broker-reported backlog for one topic subscription.
     *
     * <p>Drivers that do not expose broker-side backlog statistics retain the backward-compatible
     * {@code -1} result. Formal Pulsar backlog runs fail closed when this value is returned.
     *
     * @param topic topic whose subscription should be inspected
     * @param subscriptionName subscription name
     * @return a future containing message backlog, or {@code -1} when unsupported
     */
    default CompletableFuture<Long> getSubscriptionBacklog(String topic, String subscriptionName) {
        return CompletableFuture.completedFuture(-1L);
    }

    /**
     * Driver implementation can use this method to initialize the client libraries, with the provided
     * configuration file.
     *
     * <p>The format of the configuration file is specific to the driver implementation.
     *
     * @param configurationFile
     * @param statsLogger stats logger to collect stats from benchmark driver
     * @throws IOException
     */
    void initialize(File configurationFile, StatsLogger statsLogger)
            throws IOException, InterruptedException;

    /**
     * Get a driver specific prefix to be used in creating multiple topic names.
     *
     * @return the topic name prefix
     */
    String getTopicNamePrefix();

    /**
     * Create a new topic with a given number of partitions.
     *
     * @param topic
     * @param partitions
     * @return a future the completes when the topic is created
     */
    CompletableFuture<Void> createTopic(String topic, int partitions);

    /**
     * Create a list of new topics with the given number of partitions.
     *
     * @param topicInfos
     * @return a future the completes when the topics are created
     */
    default CompletableFuture<Void> createTopics(List<TopicInfo> topicInfos) {
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] futures =
                topicInfos.stream()
                        .map(topicInfo -> createTopic(topicInfo.getTopic(), topicInfo.getPartitions()))
                        .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    /**
     * Create a producer for a given topic.
     *
     * @param topic
     * @param options
     * @return a future for the producer
     */
    CompletableFuture<BenchmarkProducer> createProducer(String topic, ProducerOptions options);

    /**
     * Create a producer for a given topic.
     *
     * @param topic
     * @return a future for the producer
     */
    default CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        return createProducer(topic, new ProducerOptions());
    }

    /**
     * Create a producers for a given topic.
     *
     * @param producers
     * @param options
     * @return a producers future
     */
    default CompletableFuture<List<BenchmarkProducer>> createProducers(
            List<ProducerInfo> producers, ProducerOptions options) {
        List<CompletableFuture<BenchmarkProducer>> futures =
                producers.stream().map(ci -> createProducer(ci.getTopic(), options)).collect(toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(toList()));
    }

    /**
     * Create a producers for a given topic.
     *
     * @param producers
     * @return a producers future
     */
    default CompletableFuture<List<BenchmarkProducer>> createProducers(List<ProducerInfo> producers) {
        return createProducers(producers, new ProducerOptions());
    }

    /**
     * Create a benchmark consumer relative to one particular topic and subscription.
     *
     * <p>It is responsibility of the driver implementation to invoke the <code>consumerCallback
     * </code> each time a message is received.
     *
     * @param topic
     * @param subscriptionName
     * @param consumerCallback
     * @return a consumer future
     */
    CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback);

    /**
     * Create a consumers for a given topic.
     *
     * @param consumers
     * @return a consumers future
     */
    default CompletableFuture<List<BenchmarkConsumer>> createConsumers(List<ConsumerInfo> consumers) {
        List<CompletableFuture<BenchmarkConsumer>> futures =
                consumers.stream()
                        .map(
                                ci ->
                                        createConsumer(
                                                ci.getTopic(), ci.getSubscriptionName(), ci.getConsumerCallback()))
                        .collect(toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(toList()));
    }

    @Value
    class TopicInfo {
        String topic;
        int partitions;
    }

    @Value
    class ProducerInfo {
        int id;
        String topic;
    }

    @Value
    class ConsumerInfo {
        int id;
        String topic;
        String subscriptionName;
        ConsumerCallback consumerCallback;
    }
}
