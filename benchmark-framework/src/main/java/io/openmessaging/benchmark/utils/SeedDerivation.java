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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Stable, domain-separated seed derivation used by replayable campaigns. */
public final class SeedDerivation {
    private SeedDerivation() {}

    public static long derive(long baseSeed, String domain, long ordinal) {
        if (domain == null || domain.isEmpty()) {
            throw new IllegalArgumentException("seed derivation domain must not be empty");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] domainBytes = domain.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(baseSeed).array());
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(domainBytes.length).array());
            digest.update(domainBytes);
            digest.update(ByteBuffer.allocate(Long.BYTES).putLong(ordinal).array());
            byte[] bytes = digest.digest();
            return ByteBuffer.wrap(bytes, 0, Long.BYTES).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM does not provide SHA-256", e);
        }
    }
}
