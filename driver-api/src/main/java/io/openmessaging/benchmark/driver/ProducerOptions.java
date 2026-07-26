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

public class ProducerOptions {
    /** Fixed broker-side delivery delay for all messages, in milliseconds. */
    public long messageDelayMs = 0;

    /**
     * Ratio of messages that will be sent with broker-side delivery delay, between 0.0 and 1.0.
     *
     * <p>A value of 0.0 means all messages are sent without delay. A value of 1.0 means all messages
     * are delayed according to {@code messageDelayMs} or the random delay range.
     */
    public double delayMessageRatio = 0.0;

    /**
     * Minimum per-message delivery delay in milliseconds when using a random delay range.
     *
     * <p>If {@code maxMessageDelayMs > 0}, the effective delay for each message will be a random
     * value in the range [{@code minMessageDelayMs}, {@code maxMessageDelayMs}].
     */
    public long minMessageDelayMs = 0;

    /**
     * Maximum per-message delivery delay in milliseconds when using a random delay range.
     *
     * <p>If this is {@code > 0}, the benchmark will use a random delay in the range [{@code
     * minMessageDelayMs}, {@code maxMessageDelayMs}] instead of the fixed {@code messageDelayMs}.
     */
    public long maxMessageDelayMs = 0;

    @Override
    public String toString() {
        return "ProducerOptions{"
                + "messageDelayMs="
                + messageDelayMs
                + ", delayMessageRatio="
                + delayMessageRatio
                + ", minMessageDelayMs="
                + minMessageDelayMs
                + ", maxMessageDelayMs="
                + maxMessageDelayMs
                + '}';
    }
}
