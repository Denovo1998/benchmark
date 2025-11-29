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


import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ProducerOptions;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public class PulsarBenchmarkProducer implements BenchmarkProducer {

    private final Producer<byte[]> producer;
    private final ProducerOptions options;
    private final double delayMessageRatio;

    public PulsarBenchmarkProducer(Producer<byte[]> producer, ProducerOptions options) {
        this.producer = producer;
        this.options = options;
        this.delayMessageRatio = sanitizeDelayMessageRatio(options.delayMessageRatio);
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage().value(payload);
        if (key.isPresent()) {
            msgBuilder.key(key.get());
        }

        long delayMs = resolveDelayMillis();
        if (delayMs > 0 && shouldApplyDelay()) {
            msgBuilder.deliverAfter(delayMs, TimeUnit.MILLISECONDS);
        }

        return msgBuilder.sendAsync().thenApply(msgId -> null);
    }

    private long resolveDelayMillis() {
        long max = options.maxMessageDelayMs;
        long min = options.minMessageDelayMs;
        long fixed = options.messageDelayMs;

        if (max > 0) {
            long effectiveMin = min > 0 ? min : 1L;
            if (effectiveMin >= max) {
                // Degenerate range, fall back to fixed max delay
                return max;
            }
            long bound = max - effectiveMin + 1;
            return ThreadLocalRandom.current().nextLong(bound) + effectiveMin;
        }

        return fixed;
    }

    private static double sanitizeDelayMessageRatio(double ratio) {
        if (ratio <= 0.0d) {
            return 0.0d;
        }
        if (ratio >= 1.0d) {
            return 1.0d;
        }
        return ratio;
    }

    private boolean shouldApplyDelay() {
        if (delayMessageRatio <= 0.0d) {
            return false;
        }
        if (delayMessageRatio >= 1.0d) {
            return true;
        }
        return ThreadLocalRandom.current().nextDouble() < delayMessageRatio;
    }
}
