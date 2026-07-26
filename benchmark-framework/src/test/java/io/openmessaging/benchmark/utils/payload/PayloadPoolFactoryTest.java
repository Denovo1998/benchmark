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
package io.openmessaging.benchmark.utils.payload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.openmessaging.benchmark.worker.commands.PayloadSpec;
import io.openmessaging.benchmark.worker.commands.PayloadSpec.PayloadMode;
import java.util.List;
import org.junit.jupiter.api.Test;

class PayloadPoolFactoryTest {
    @Test
    void randomizedPoolIsDeterministic() {
        PayloadSpec spec = new PayloadSpec();
        spec.mode = PayloadMode.RANDOMIZED;
        spec.messageSize = 8;
        spec.randomBytesRatio = 0.5;
        spec.poolSize = 3;
        spec.seed = 7;

        List<byte[]> first = PayloadPoolFactory.create(spec);
        List<byte[]> second = PayloadPoolFactory.create(spec);

        assertThat(PayloadPoolFactory.checksum(first)).isEqualTo(PayloadPoolFactory.checksum(second));
        for (int i = 0; i < first.size(); i++) {
            assertThat(first.get(i)).containsExactly(second.get(i));
        }
    }

    @Test
    void checksumMismatchFailsClosed() {
        PayloadSpec spec = new PayloadSpec();
        spec.mode = PayloadMode.RANDOMIZED;
        spec.messageSize = 4;
        spec.randomBytesRatio = 1.0;
        spec.poolSize = 1;
        spec.seed = 3;
        spec.expectedSha256 = "incorrect";

        assertThatIllegalArgumentException().isThrownBy(() -> PayloadPoolFactory.create(spec));
    }
}
