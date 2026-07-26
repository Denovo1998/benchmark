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

import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;

public class Workload {
    public String name;

    /** Number of topics to create in the test. */
    public int topics;

    /** Number of partitions each topic will contain. */
    public int partitionsPerTopic;

    public KeyDistributorType keyDistributor = KeyDistributorType.NO_KEY;

    public int messageSize;

    public boolean useRandomizedPayloads;
    public double randomBytesRatio;
    public int randomizedPayloadPoolSize;

    public String payloadFile;

    public int subscriptionsPerTopic;

    public int producersPerTopic;

    public int consumerPerSubscription;

    public int producerRate;

    /**
     * If the consumer backlog is > 0, the generator will accumulate messages until the requested
     * amount of storage is retained and then it will start the consumers to drain it.
     *
     * <p>The testDurationMinutes will be overruled to allow the test to complete when the consumer
     * has drained all the backlog and it's on par with the producer
     */
    public long consumerBacklogSizeGB = 0;

    /**
     * The ratio of the backlog that can remain and yet the backlog still be considered empty, and
     * thus the workload can complete at the end of the configured duration. In some systems it is not
     * feasible for the backlog to be drained fully and thus the workload will run indefinitely. In
     * such circumstances, one may be content to achieve a partial drain such as 99% of the backlog.
     * The value should be on somewhere between 0.0 and 1.0, where 1.0 indicates that the backlog
     * should be fully drained, and 0.0 indicates a best effort, where the workload will complete
     * after the specified time irrespective of how much of the backlog has been drained.
     */
    public double backlogDrainRatio = 1.0;

    public int testDurationMinutes;

    public int warmupDurationMinutes = 1;

    /** Sampling interval used by the run manifest and result samples. */
    public int statsIntervalSeconds = 10;

    /** Fixed broker-side delivery delay for all messages, in milliseconds. */
    public long messageDelayMs = 0;

    /**
     * Ratio of messages that will be sent with broker-side delivery delay, between 0.0 and 1.0.
     *
     * <p>A value of 0.0 means all messages are sent without delay. A value of 1.0 means all messages
     * are delayed according to {@code messageDelayMs} or the random delay range. Values outside [0.0,
     * 1.0] are not validated here and may be sanitized by individual drivers.
     */
    public double delayMessageRatio = 0.0;

    /**
     * Minimum per-message delivery delay in milliseconds when using a random delay range.
     *
     * <p>If {@code maxMessageDelayMs > 0}, the effective delay for each message will be a random
     * value in the range [{@code minMessageDelayMs}, {@code maxMessageDelayMs}]. If {@code
     * minMessageDelayMs} is 0 or negative, it will be treated as 1 millisecond.
     */
    public long minMessageDelayMs = 0;

    /**
     * Maximum per-message delivery delay in milliseconds when using a random delay range.
     *
     * <p>If this is {@code > 0}, the benchmark will use a random delay in the range [{@code
     * minMessageDelayMs}, {@code maxMessageDelayMs}] instead of the fixed {@code messageDelayMs}.
     */
    public long maxMessageDelayMs = 0;

    public void validate() {
        if (topics <= 0 || partitionsPerTopic <= 0) {
            throw new IllegalArgumentException("topics and partitionsPerTopic must be positive");
        }
        if (messageSize <= 0) {
            throw new IllegalArgumentException("messageSize must be positive");
        }
        if (subscriptionsPerTopic <= 0 || producersPerTopic <= 0 || consumerPerSubscription <= 0) {
            throw new IllegalArgumentException("producer and consumer counts must be positive");
        }
        if (producerRate < 0 || testDurationMinutes < 0 || warmupDurationMinutes < 0) {
            throw new IllegalArgumentException("durations and producerRate must not be negative");
        }
        if (statsIntervalSeconds <= 0) {
            throw new IllegalArgumentException("statsIntervalSeconds must be positive");
        }
        if (useRandomizedPayloads) {
            if (randomBytesRatio < 0.0 || randomBytesRatio > 1.0) {
                throw new IllegalArgumentException("randomBytesRatio must be between 0 and 1");
            }
            if (randomizedPayloadPoolSize <= 0) {
                throw new IllegalArgumentException("randomizedPayloadPoolSize must be positive");
            }
            if (payloadFile != null && !payloadFile.isEmpty()) {
                throw new IllegalArgumentException(
                        "payloadFile must be empty when useRandomizedPayloads is enabled");
            }
        }
        if (backlogDrainRatio < 0.0 || backlogDrainRatio > 1.0) {
            throw new IllegalArgumentException("backlogDrainRatio must be between 0 and 1");
        }
    }
}
