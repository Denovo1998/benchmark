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
package io.openmessaging.benchmark.worker.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CountersStatsTest {

    @Test
    void plus() {
        CountersStats one = new CountersStats();
        one.messagesSent = 1;
        one.messageSendErrors = 10;
        one.messagesReceived = 100;
        one.inFlightSends = 1000;
        one.messagesAcknowledged = 90;
        one.ackErrors = 3;
        one.ackInFlight = 7;
        CountersStats two = new CountersStats();
        two.messagesSent = 2;
        two.messageSendErrors = 20;
        two.messagesReceived = 200;
        two.inFlightSends = 2000;
        two.messagesAcknowledged = 180;
        two.ackErrors = 4;
        two.ackInFlight = 16;
        two.acknowledgementTrackingSupported = true;

        CountersStats result = one.plus(two);
        assertThat(result)
                .satisfies(
                        r -> {
                            assertThat(r.messagesSent).isEqualTo(3);
                            assertThat(r.messageSendErrors).isEqualTo(30);
                            assertThat(r.messagesReceived).isEqualTo(300);
                            assertThat(r.inFlightSends).isEqualTo(3000);
                            assertThat(r.messagesAcknowledged).isEqualTo(270);
                            assertThat(r.ackErrors).isEqualTo(7);
                            assertThat(r.ackInFlight).isEqualTo(23);
                            assertThat(r.acknowledgementTrackingSupported).isTrue();
                        });
    }

    @Test
    void zeroPlus() {
        CountersStats zero = new CountersStats();
        CountersStats two = new CountersStats();
        two.messagesSent = 2;
        two.messageSendErrors = 20;
        two.messagesReceived = 200;
        two.messagesAcknowledged = 190;
        two.ackInFlight = 10;
        two.acknowledgementTrackingSupported = true;

        CountersStats result = zero.plus(two);
        assertThat(result)
                .satisfies(
                        r -> {
                            assertThat(r.messagesSent).isEqualTo(2);
                            assertThat(r.messageSendErrors).isEqualTo(20);
                            assertThat(r.messagesReceived).isEqualTo(200);
                            assertThat(r.messagesAcknowledged).isEqualTo(190);
                            assertThat(r.ackInFlight).isEqualTo(10);
                            assertThat(r.acknowledgementTrackingSupported).isTrue();
                        });
    }
}
