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

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;

import com.beust.jcommander.internal.Maps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.driver.DriverRuntimeInfo;
import io.openmessaging.benchmark.utils.ListPartition;
import io.openmessaging.benchmark.utils.SeedDerivation;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedWorkersEnsemble implements Worker {
    private final Thread shutdownHook = new Thread(this::stopAll);
    private final List<Worker> workers;
    private final List<Worker> producerWorkers;
    private final List<Worker> consumerWorkers;
    private final Worker leader;

    private int numberOfUsedProducerWorkers;

    public DistributedWorkersEnsemble(List<Worker> workers, boolean extraConsumerWorkers) {
        Preconditions.checkArgument(workers.size() > 1);
        this.workers = unmodifiableList(workers);
        leader = workers.get(0);
        int numberOfProducerWorkers = getNumberOfProducerWorkers(workers, extraConsumerWorkers);
        List<List<Worker>> partitions =
                Lists.partition(Lists.reverse(workers), workers.size() - numberOfProducerWorkers);
        this.producerWorkers = partitions.get(1);
        this.consumerWorkers = partitions.get(0);

        log.info(
                "Workers list - producers: [{}]",
                producerWorkers.stream().map(Worker::id).collect(joining(",")));
        log.info(
                "Workers list - consumers: {}",
                consumerWorkers.stream().map(Worker::id).collect(joining(",")));

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /*
     * For driver-jms extra consumers are required. If there is an odd number of workers then allocate the extra
     * to consumption.
     */
    @VisibleForTesting
    static int getNumberOfProducerWorkers(List<Worker> workers, boolean extraConsumerWorkers) {
        return extraConsumerWorkers ? (workers.size() + 2) / 3 : workers.size() / 2;
    }

    @Override
    public void initializeDriver(File configurationFile) throws IOException {
        workers.parallelStream()
                .forEach(
                        w -> {
                            try {
                                w.initializeDriver(configurationFile);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public DriverRuntimeInfo getDriverRuntimeInfo() throws IOException {
        List<DriverRuntimeInfo> infos =
                workers.parallelStream()
                        .map(
                                w -> {
                                    try {
                                        return w.getDriverRuntimeInfo();
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(java.util.stream.Collectors.toList());
        DriverRuntimeInfo leaderInfo = infos.get(0);
        for (DriverRuntimeInfo info : infos) {
            if (!runtimeInfoEquals(leaderInfo, info)) {
                throw new IllegalStateException(
                        "Distributed workers reported different driver runtime identities: "
                                + leaderInfo.namespace
                                + " vs "
                                + info.namespace);
            }
        }
        return leaderInfo;
    }

    @Override
    public long getSubscriptionBacklog(String topic, String subscriptionName) throws IOException {
        return leader.getSubscriptionBacklog(topic, subscriptionName);
    }

    /**
     * Return the stable worker role assignment used for the current run.
     *
     * @return worker id to role mapping
     */
    public Map<String, String> getWorkerRoleAssignment() {
        Map<String, String> roles = new LinkedHashMap<>();
        for (Worker worker : producerWorkers) {
            roles.put(worker.id(), "producer");
        }
        for (Worker worker : consumerWorkers) {
            roles.put(worker.id(), "consumer");
        }
        return roles;
    }

    private static boolean runtimeInfoEquals(DriverRuntimeInfo left, DriverRuntimeInfo right) {
        return Objects.equals(left.namespace, right.namespace)
                && Objects.equals(left.attributes, right.attributes)
                && runEquals(left.run, right.run);
    }

    private static boolean runEquals(
            io.openmessaging.benchmark.driver.RunConfiguration left,
            io.openmessaging.benchmark.driver.RunConfiguration right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        return Objects.equals(left.campaignId, right.campaignId)
                && Objects.equals(left.blockId, right.blockId)
                && Objects.equals(left.runId, right.runId)
                && Objects.equals(left.stage, right.stage)
                && left.repetition == right.repetition
                && Objects.equals(left.seed, right.seed);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> createTopics(TopicsInfo topicsInfo) throws IOException {
        return leader.createTopics(topicsInfo);
    }

    @Override
    public void createProducers(List<String> topics, Workload workload) {
        List<List<String>> topicsPerProducer =
                ListPartition.partitionList(topics, producerWorkers.size());
        Map<Worker, List<String>> topicsPerProducerMap = Maps.newHashMap();
        int i = 0;
        for (List<String> assignedTopics : topicsPerProducer) {
            topicsPerProducerMap.put(producerWorkers.get(i++), assignedTopics);
        }

        // Number of actually used workers might be less than available workers
        numberOfUsedProducerWorkers =
                (int) topicsPerProducerMap.values().stream().filter(t -> !t.isEmpty()).count();
        log.debug(
                "Producing worker count: {} of {}", numberOfUsedProducerWorkers, producerWorkers.size());
        topicsPerProducerMap.entrySet().parallelStream()
                .forEach(
                        e -> {
                            try {
                                e.getKey().createProducers(e.getValue(), workload);
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        });
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) throws IOException {
        // Reduce the publish rate across all the brokers
        double newRate = producerWorkAssignment.publishRate / numberOfUsedProducerWorkers;
        log.debug("Setting worker assigned publish rate to {} msgs/sec", newRate);
        // Reduce the publish rate across all the brokers
        java.util.stream.IntStream.range(0, producerWorkers.size())
                .parallel()
                .forEach(
                        index -> {
                            Worker w = producerWorkers.get(index);
                            try {
                                w.startLoad(
                                        producerWorkAssignment
                                                .withPublishRate(newRate)
                                                .withPayloadSelectionSeed(
                                                        SeedDerivation.derive(
                                                                producerWorkAssignment.payloadSelectionSeed,
                                                                "worker-payload-selection",
                                                                index)));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void probeProducers() throws IOException {
        producerWorkers.parallelStream()
                .forEach(
                        w -> {
                            try {
                                w.probeProducers();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void adjustPublishRate(double publishRate) throws IOException {
        double newRate = publishRate / numberOfUsedProducerWorkers;
        log.debug("Adjusting producer publish rate to {} msgs/sec", newRate);
        producerWorkers.parallelStream()
                .forEach(
                        w -> {
                            try {
                                w.adjustPublishRate(newRate);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void stopAll() {
        workers.parallelStream().forEach(Worker::stopAll);
    }

    @Override
    public String id() {
        return "Ensemble[" + workers.stream().map(Worker::id).collect(joining(",")) + "]";
    }

    @Override
    public void pauseConsumers() throws IOException {
        consumerWorkers.parallelStream()
                .forEach(
                        w -> {
                            try {
                                w.pauseConsumers();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void resumeConsumers() throws IOException {
        consumerWorkers.parallelStream()
                .forEach(
                        w -> {
                            try {
                                w.resumeConsumers();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void createConsumers(ConsumerAssignment overallConsumerAssignment) {
        List<List<TopicSubscription>> subscriptionsPerConsumer =
                ListPartition.partitionList(
                        overallConsumerAssignment.topicsSubscriptions, consumerWorkers.size());
        Map<Worker, ConsumerAssignment> topicsPerWorkerMap = Maps.newHashMap();
        int i = 0;
        for (List<TopicSubscription> tsl : subscriptionsPerConsumer) {
            ConsumerAssignment individualAssignment = new ConsumerAssignment();
            individualAssignment.topicsSubscriptions = tsl;
            topicsPerWorkerMap.put(consumerWorkers.get(i++), individualAssignment);
        }
        topicsPerWorkerMap.entrySet().parallelStream()
                .forEach(
                        e -> {
                            try {
                                e.getKey().createConsumers(e.getValue());
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        });
    }

    @Override
    public PeriodStats getPeriodStats() {
        return workers.parallelStream()
                .map(
                        w -> {
                            try {
                                return w.getPeriodStats();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .reduce(new PeriodStats(), PeriodStats::plus);
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        return workers.parallelStream()
                .map(
                        w -> {
                            try {
                                return w.getCumulativeLatencies();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .reduce(new CumulativeLatencies(), CumulativeLatencies::plus);
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        return workers.parallelStream()
                .map(
                        w -> {
                            try {
                                return w.getCountersStats();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .reduce(new CountersStats(), CountersStats::plus);
    }

    @Override
    public void resetStats() throws IOException {
        workers.parallelStream()
                .forEach(
                        w -> {
                            try {
                                w.resetStats();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    @Override
    public void close() throws Exception {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        for (Worker w : workers) {
            try {
                w.close();
            } catch (Exception ignored) {
                log.trace("Ignored error while closing worker {}", w, ignored);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DistributedWorkersEnsemble.class);
}
