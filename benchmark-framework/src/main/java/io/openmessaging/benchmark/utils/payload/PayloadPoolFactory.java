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

import io.openmessaging.benchmark.worker.commands.PayloadSpec;
import io.openmessaging.benchmark.worker.commands.PayloadSpec.PayloadMode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Generates and verifies worker-local payload pools. */
public final class PayloadPoolFactory {
    private PayloadPoolFactory() {}

    public static List<byte[]> create(PayloadSpec spec) {
        validate(spec);
        List<byte[]> payloads = new ArrayList<>();
        if (spec.mode == PayloadMode.INLINE) {
            payloads.add(spec.inlinePayload.clone());
            verifyChecksum(spec, payloads);
            return payloads;
        }

        int randomBytes = (int) (spec.messageSize * spec.randomBytesRatio);
        int zeroBytes = spec.messageSize - randomBytes;
        Random random = new Random(spec.seed);
        for (int i = 0; i < spec.poolSize; i++) {
            byte[] payload = new byte[spec.messageSize];
            random.nextBytes(payload);
            for (int j = randomBytes; j < randomBytes + zeroBytes; j++) {
                payload[j] = 0;
            }
            payloads.add(payload);
        }
        verifyChecksum(spec, payloads);
        return payloads;
    }

    public static String checksum(List<byte[]> payloads) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (byte[] payload : payloads) {
                digest.update(payload);
            }
            byte[] result = digest.digest();
            StringBuilder hex = new StringBuilder(result.length * 2);
            for (byte b : result) {
                hex.append(String.format("%02x", b & 0xff));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM does not provide SHA-256", e);
        }
    }

    private static void verifyChecksum(PayloadSpec spec, List<byte[]> payloads) {
        String actual = spec.expectedSha256 == null ? null : checksum(payloads);
        if (actual != null && !spec.expectedSha256.equals(actual)) {
            throw new IllegalArgumentException(
                    "payload checksum mismatch: expected=" + spec.expectedSha256 + ", actual=" + actual);
        }
    }

    private static void validate(PayloadSpec spec) {
        if (spec == null || spec.mode == null) {
            throw new IllegalArgumentException("payload spec is required");
        }
        if (spec.messageSize <= 0) {
            throw new IllegalArgumentException("payload messageSize must be positive");
        }
        if (spec.mode == PayloadMode.RANDOMIZED) {
            if (spec.poolSize <= 0) {
                throw new IllegalArgumentException("randomized payload poolSize must be positive");
            }
            if (spec.randomBytesRatio < 0.0 || spec.randomBytesRatio > 1.0) {
                throw new IllegalArgumentException("randomBytesRatio must be between 0 and 1");
            }
        } else if (spec.inlinePayload == null || spec.inlinePayload.length != spec.messageSize) {
            throw new IllegalArgumentException("inline payload size must equal messageSize");
        }
    }
}
