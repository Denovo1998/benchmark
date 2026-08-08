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
package io.openmessaging.benchmark;

import io.openmessaging.benchmark.driver.DriverRuntimeInfo;
import io.openmessaging.benchmark.driver.RunConfiguration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Evidence envelope tying a result to the exact campaign identity and runtime. */
public class RunManifest {
    public String schemaVersion = "1";
    public RunConfiguration run;
    public String driverName;
    public String workloadName;
    public String driverConfigPath;
    public String workloadPath;
    public String driverConfigSha256;
    public String workloadSha256;
    public String startedAt;
    public String measurementStartedAt;
    public double measurementDurationSeconds;
    public String completedAt;
    public String endedAt;
    public String status = "PREPARED";
    public String failureType;
    public String failureMessage;
    public String resultPath;
    public String workerId;
    public List<String> workerUrls = new ArrayList<>();
    public int workerCount;
    public Map<String, String> workerRoleAssignment = new LinkedHashMap<>();
    public String ombGitSha;
    public String ombImage;
    public String ombImageDigest;
    public String ombRuntimeConfigId;
    public String kubernetesContext;
    public String kubernetesNamespace;
    public String pulsarRelease;
    public String pulsarCluster;
    public String deploymentRunEnvPath;
    public String deploymentRunEnvSha256;
    public String helmEvidenceArchivePath;
    public String helmEvidenceArchiveSha256;
    public String pulsarStage;
    public String brokerImageId;
    public String nereusSourceIdentity;
    public String payloadMode;
    public int payloadMessageSize;
    public double payloadRandomBytesRatio;
    public int payloadPoolSize;
    public long payloadSeed;
    public String payloadSha256;
    public String assignmentSha256;
    public boolean warmupDrainApplied;
    public double warmupDrainDurationSeconds;
    public long warmupDrainMessagesSent;
    public long warmupDrainMessagesReceived;
    public long warmupDrainMessageSendErrors;
    public long warmupDrainInFlightSends;
    public long warmupDrainMessagesAcknowledged;
    public long warmupDrainAckErrors;
    public long warmupDrainAckInFlight;
    public boolean warmupDrainAcknowledgementTrackingSupported;
    public long warmupDrainBacklogMessages;
    public Long warmupDrainBrokerBacklogMessages;
    public int warmupDrainBrokerBacklogZeroPolls;
    public String measurementEndedAt;
    public String measurementCompletedAt;
    public boolean measurementDrainApplied;
    public double measurementDrainDurationSeconds;
    public long measurementDrainMessagesSent;
    public long measurementDrainMessagesReceived;
    public long measurementDrainMessageSendErrors;
    public long measurementDrainInFlightSends;
    public long measurementDrainMessagesAcknowledged;
    public long measurementDrainAckErrors;
    public long measurementDrainAckInFlight;
    public boolean measurementDrainAcknowledgementTrackingSupported;
    public long measurementDrainBacklogMessages;
    public Long measurementDrainBrokerBacklogMessages;
    public int measurementDrainBrokerBacklogZeroPolls;
    public double targetPublishRate;
    public long requestedBacklogBytes;
    public long backlogAtDrainStartMessages;
    public double backlogBuildDurationSeconds;
    public double backlogDrainDurationSeconds;
    public double averageDrainRateMessagesPerSecond;
    public double peakDrainRateMessagesPerSecond;
    public long postDrainBacklogMessages;
    public Long brokerBacklogAtDrainStartMessages;
    public Long brokerBacklogAfterDrainMessages;
    public String backlogPhase;
    public DriverRuntimeInfo runtimeInfo;
    public List<PeriodSample> samples = new ArrayList<>();
}
