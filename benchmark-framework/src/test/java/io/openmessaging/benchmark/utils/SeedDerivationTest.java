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
package io.openmessaging.benchmark.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SeedDerivationTest {
    @Test
    void vectorsRemainStable() {
        assertThat(SeedDerivation.derive(202607250101L, "payload-pool", 0))
                .isEqualTo(3220135462016411106L);
        assertThat(SeedDerivation.derive(202607250101L, "consumer-assignment", 0))
                .isEqualTo(-30921928482582112L);
        assertThat(SeedDerivation.derive(-1L, "topic-name", 7)).isEqualTo(2358284276144299481L);
    }

    @Test
    void domainsAndOrdinalsDoNotCollideByConstruction() {
        assertThat(SeedDerivation.derive(1L, "payload-pool", 0))
                .isNotEqualTo(SeedDerivation.derive(1L, "payload-pool", 1));
        assertThat(SeedDerivation.derive(1L, "payload-pool", 0))
                .isNotEqualTo(SeedDerivation.derive(1L, "topic-name", 0));
    }
}
