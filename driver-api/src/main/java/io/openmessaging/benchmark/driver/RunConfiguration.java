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

/** Immutable identity supplied by a benchmark campaign. */
public class RunConfiguration {
    public String campaignId;
    public String blockId;
    public String runId;
    public String stage;
    public int repetition;
    public Long seed;

    public boolean isConfigured() {
        return campaignId != null
                && !campaignId.isEmpty()
                && blockId != null
                && !blockId.isEmpty()
                && runId != null
                && !runId.isEmpty()
                && stage != null
                && !stage.isEmpty()
                && repetition > 0
                && seed != null;
    }
}
