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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.openmessaging.benchmark.driver.ProducerOptions;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;

class PulsarBenchmarkDriverTest {

    @Test
    void shouldAllowDelayedDeliveryForSharedSubscription() {
        ProducerOptions options = new ProducerOptions();
        options.delayMessageRatio = 1.0d;
        options.maxMessageDelayMs = 20_000L;

        assertThatCode(
                        () ->
                                PulsarBenchmarkDriver.validateDelayedDeliveryConfiguration(
                                        options, SubscriptionType.Shared))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectMissingDelayDurationWhenDelayRatioIsEnabled() {
        ProducerOptions options = new ProducerOptions();
        options.delayMessageRatio = 1.0d;

        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                PulsarBenchmarkDriver.validateDelayedDeliveryConfiguration(
                                        options, SubscriptionType.Shared))
                .withMessageContaining("delayMessageRatio > 0")
                .withMessageContaining("messageDelayMs")
                .withMessageContaining("maxMessageDelayMs");
    }

    @Test
    void shouldRejectUnsupportedSubscriptionTypeForDelayedDelivery() {
        ProducerOptions options = new ProducerOptions();
        options.delayMessageRatio = 1.0d;
        options.messageDelayMs = 10_000L;

        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                PulsarBenchmarkDriver.validateDelayedDeliveryConfiguration(
                                        options, SubscriptionType.Failover))
                .withMessageContaining("Shared or Key_Shared")
                .withMessageContaining("Failover");
    }
}
