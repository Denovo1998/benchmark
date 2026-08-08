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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ProducerOptions;
import io.openmessaging.benchmark.driver.RunConfiguration;
import io.openmessaging.benchmark.driver.pulsar.config.PulsarConfig;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException.ServerSideErrorException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class PulsarBenchmarkDriverTest {

    @Test
    @SuppressWarnings("unchecked")
    void tracksAcknowledgementFromListenerEntryUntilFutureCompletion() {
        Consumer<ByteBuffer> consumer = mock(Consumer.class);
        Message<ByteBuffer> message = mock(Message.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        ByteBuffer payload = ByteBuffer.wrap(new byte[] {1, 2, 3});
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        when(message.getValue()).thenReturn(payload);
        when(message.getPublishTime()).thenReturn(123L);
        when(consumer.acknowledgeAsync(message)).thenReturn(acknowledgement);

        PulsarBenchmarkDriver.receiveAndAcknowledge(consumer, message, callback);

        InOrder order = inOrder(callback, consumer, message);
        order.verify(callback).messageAcknowledgementStarted();
        order.verify(callback).messageReceived(payload, 123L);
        order.verify(consumer).acknowledgeAsync(message);
        order.verify(message).release();
        verify(callback, never()).messageAcknowledgementCompleted(any());

        acknowledgement.complete(null);

        verify(callback).messageAcknowledgementCompleted(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsAsynchronousAcknowledgementFailure() {
        Consumer<ByteBuffer> consumer = mock(Consumer.class);
        Message<ByteBuffer> message = mock(Message.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        IllegalStateException failure = new IllegalStateException("ack failed");
        when(message.getValue()).thenReturn(ByteBuffer.allocate(1));
        when(consumer.acknowledgeAsync(message)).thenReturn(acknowledgement);

        PulsarBenchmarkDriver.receiveAndAcknowledge(consumer, message, callback);
        acknowledgement.completeExceptionally(failure);

        verify(callback).messageAcknowledgementCompleted(failure);
        verify(message).release();
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsSynchronousListenerFailureWithoutSchedulingAcknowledgement() {
        Consumer<ByteBuffer> consumer = mock(Consumer.class);
        Message<ByteBuffer> message = mock(Message.class);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        IllegalStateException failure = new IllegalStateException("listener failed");
        ByteBuffer payload = ByteBuffer.allocate(1);
        when(message.getValue()).thenReturn(payload);
        when(message.getPublishTime()).thenReturn(123L);
        doThrow(failure).when(callback).messageReceived(eq(payload), eq(123L));

        PulsarBenchmarkDriver.receiveAndAcknowledge(consumer, message, callback);

        verify(callback).messageAcknowledgementStarted();
        verify(callback).messageAcknowledgementCompleted(failure);
        verify(consumer, never()).acknowledgeAsync(any(Message.class));
        verify(message).release();
    }

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

    @Test
    void shouldComparePersistenceStorageClassByValue() {
        PersistencePolicies expected = new PersistencePolicies(3, 3, 2, 1.0, "nereus");
        PersistencePolicies actual = new PersistencePolicies(3, 3, 2, 1.0, new String("nereus"));

        assertThat(PulsarBenchmarkDriver.verifyPersistencePolicy(expected, actual)).isTrue();
    }

    @Test
    void shouldRejectAnyPersistenceFieldMismatch() {
        PersistencePolicies expected = new PersistencePolicies(3, 3, 2, 1.0, "bookkeeper");
        assertThat(
                        PulsarBenchmarkDriver.verifyPersistencePolicy(
                                expected, new PersistencePolicies(2, 3, 2, 1.0, "bookkeeper")))
                .isFalse();
        assertThat(
                        PulsarBenchmarkDriver.verifyPersistencePolicy(
                                expected, new PersistencePolicies(3, 2, 2, 1.0, "bookkeeper")))
                .isFalse();
        assertThat(
                        PulsarBenchmarkDriver.verifyPersistencePolicy(
                                expected, new PersistencePolicies(3, 3, 1, 1.0, "bookkeeper")))
                .isFalse();
        assertThat(
                        PulsarBenchmarkDriver.verifyPersistencePolicy(
                                expected, new PersistencePolicies(3, 3, 2, 2.0, "bookkeeper")))
                .isFalse();
        assertThat(
                        PulsarBenchmarkDriver.verifyPersistencePolicy(
                                expected, new PersistencePolicies(3, 3, 2, 1.0, "nereus")))
                .isFalse();
    }

    @Test
    void shouldRetryNereusNamespacePolicyVersionConflictOnly() {
        ServerSideErrorException versionChanged =
                new ServerSideErrorException(
                        new IllegalStateException("NEREUS_NAMESPACE_POLICY_VERSION_CHANGED"),
                        "NEREUS_NAMESPACE_POLICY_VERSION_CHANGED",
                        "NEREUS_NAMESPACE_POLICY_VERSION_CHANGED",
                        500);
        ServerSideErrorException unrelatedServerError =
                new ServerSideErrorException(
                        new IllegalStateException("unrelated server error"),
                        "unrelated server error",
                        "unrelated server error",
                        500);

        assertThat(PulsarBenchmarkDriver.isTransientPersistenceConflict(versionChanged)).isTrue();
        assertThat(PulsarBenchmarkDriver.isTransientPersistenceConflict(unrelatedServerError))
                .isFalse();
    }

    @Test
    void shouldRecognizeOnlyOxiaAdminPolicyAlreadyExistsAsConcurrentCreateConflict() {
        ServerSideErrorException oxiaAlreadyExists =
                new ServerSideErrorException(
                        new IllegalStateException("key already exists: /admin/policies/benchmark"),
                        "key already exists: /admin/policies/benchmark",
                        "key already exists: /admin/policies/benchmark",
                        500);
        ServerSideErrorException unrelatedAlreadyExists =
                new ServerSideErrorException(
                        new IllegalStateException("key already exists: /other/path"),
                        "key already exists: /other/path",
                        "key already exists: /other/path",
                        500);

        assertThat(PulsarBenchmarkDriver.isConcurrentAdminCreateConflict(oxiaAlreadyExists)).isTrue();
        assertThat(PulsarBenchmarkDriver.isConcurrentAdminCreateConflict(unrelatedAlreadyExists))
                .isFalse();
    }

    @Test
    void shouldRetryOnlyTopicDoesNotExistFailures() {
        assertThat(
                        PulsarBenchmarkDriver.isTopicNotReady(
                                new PulsarClientException.TopicDoesNotExistException("topic")))
                .isTrue();
        assertThat(PulsarBenchmarkDriver.isTopicNotReady(new IllegalStateException("topic"))).isFalse();
    }

    @Test
    void shouldRequireStageStorageClassMapping() {
        PulsarConfig config = formalConfig("C", "nereus");

        assertThatCode(() -> PulsarBenchmarkDriver.validateConfiguration(config))
                .doesNotThrowAnyException();

        config.client.persistence.managedLedgerStorageClassName = "bookkeeper";
        assertThatIllegalArgumentException()
                .isThrownBy(() -> PulsarBenchmarkDriver.validateConfiguration(config))
                .withMessageContaining("stage C")
                .withMessageContaining("nereus");
    }

    @Test
    void shouldAcceptBothBookkeeperAndNereusStageMappings() {
        assertThatCode(
                        () -> PulsarBenchmarkDriver.validateConfiguration(formalConfig("A", "bookkeeper")))
                .doesNotThrowAnyException();
        assertThatCode(() -> PulsarBenchmarkDriver.validateConfiguration(formalConfig("E", "nereus")))
                .doesNotThrowAnyException();
    }

    private static PulsarConfig formalConfig(String stage, String storageClass) {
        PulsarConfig config = new PulsarConfig();
        config.client.namespacePrefix = "benchmark/v010";
        config.client.namespaceSuffix = "run-01";
        config.client.persistence.managedLedgerStorageClassName = storageClass;
        config.run = new RunConfiguration();
        config.run.campaignId = "campaign";
        config.run.blockId = "block";
        config.run.runId = "run-01";
        config.run.stage = stage;
        config.run.repetition = 1;
        config.run.seed = 1L;
        return config;
    }
}
