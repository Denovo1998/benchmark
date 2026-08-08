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

import static io.openmessaging.benchmark.utils.UniformRateLimiter.uninterruptibleSleepNs;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.utils.UniformRateLimiter;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer {

    private final WorkerStats stats;
    private UniformRateLimiter rateLimiter;
    private Supplier<Long> nanoClock;

    MessageProducer(UniformRateLimiter rateLimiter, WorkerStats stats) {
        this(System::nanoTime, rateLimiter, stats);
    }

    MessageProducer(Supplier<Long> nanoClock, UniformRateLimiter rateLimiter, WorkerStats stats) {
        this.nanoClock = nanoClock;
        this.rateLimiter = rateLimiter;
        this.stats = stats;
    }

    public void sendMessage(BenchmarkProducer producer, Optional<String> key, byte[] payload) {
        sendMessage(producer, key, payload, () -> false);
    }

    void sendMessage(
            BenchmarkProducer producer, Optional<String> key, byte[] payload, BooleanSupplier cancelled) {
        final long intendedSendTime = rateLimiter.acquire();
        uninterruptibleSleepNs(intendedSendTime);
        if (cancelled.getAsBoolean()) {
            return;
        }
        final long sendTime = nanoClock.get();
        stats.recordProducerStarted();
        try {
            producer
                    .sendAsync(key, payload)
                    .whenComplete(
                            (ignored, error) -> {
                                if (error == null) {
                                    success(payload.length, intendedSendTime, sendTime);
                                } else {
                                    failure(error);
                                }
                            });
        } catch (Throwable t) {
            failure(t);
        }
    }

    private void success(long payloadLength, long intendedSendTime, long sendTime) {
        long nowNs = nanoClock.get();
        stats.recordProducerSuccess(payloadLength, intendedSendTime, sendTime, nowNs);
    }

    private void failure(Throwable t) {
        stats.recordProducerFailure();
        log.warn("Write error on message", t);
    }

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);
}
