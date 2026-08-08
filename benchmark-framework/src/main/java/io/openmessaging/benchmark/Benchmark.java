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

import static java.util.stream.Collectors.toList;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.worker.DistributedWorkersEnsemble;
import io.openmessaging.benchmark.worker.HttpWorkerClient;
import io.openmessaging.benchmark.worker.LocalWorker;
import io.openmessaging.benchmark.worker.Worker;
import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Benchmark {

    static class Arguments {

        @Parameter(
                names = {"-c", "--csv"},
                description = "Print results from this directory to a csv file")
        String resultsDir;

        @Parameter(
                names = {"-h", "--help"},
                description = "Help message",
                help = true)
        boolean help;

        @Parameter(
                names = {"-d", "--drivers"},
                description =
                        "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml") // , required = true)
        public List<String> drivers;

        @Parameter(
                names = {"-w", "--workers"},
                description = "List of worker nodes. eg: http://1.2.3.4:8080,http://4.5.6.7:8080")
        public List<String> workers;

        @Parameter(
                names = {"-wf", "--workers-file"},
                description = "Path to a YAML file containing the list of workers addresses")
        public File workersFile;

        @Parameter(
                names = {"-x", "--extra"},
                description = "Allocate extra consumer workers when your backlog builds.")
        boolean extraConsumers;

        @Parameter(description = "Workloads") // , required = true)
        public List<String> workloads;

        @Parameter(
                names = {"-o", "--output"},
                description = "Output",
                required = false)
        public String output;
    }

    @SuppressWarnings("checkstyle:MethodLength")
    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("messaging-benchmark");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        if (arguments.resultsDir != null) {
            ResultsToCsv r = new ResultsToCsv();
            r.writeAllResultFiles(arguments.resultsDir);
            System.exit(0);
        }

        if (arguments.workers != null && arguments.workersFile != null) {
            System.err.println("Only one between --workers and --workers-file can be specified");
            System.exit(-1);
        }

        if (arguments.workers == null && arguments.workersFile == null) {
            File defaultFile = new File("workers.yaml");
            if (defaultFile.exists()) {
                log.info("Using default worker file workers.yaml");
                arguments.workersFile = defaultFile;
            }
        }

        if (arguments.workersFile != null) {
            log.info("Reading workers list from {}", arguments.workersFile);
            arguments.workers = mapper.readValue(arguments.workersFile, Workers.class).workers;
        }

        // Dump configuration variables
        log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));

        if (arguments.drivers == null
                || arguments.drivers.isEmpty()
                || arguments.workloads == null
                || arguments.workloads.isEmpty()) {
            throw new IllegalArgumentException("At least one driver and workload must be specified");
        }

        Map<String, Workload> workloads = new TreeMap<>();
        Map<String, File> workloadFiles = new TreeMap<>();
        for (String path : arguments.workloads) {
            File file = new File(path);
            String name = file.getName().substring(0, file.getName().lastIndexOf('.'));

            workloads.put(name, mapper.readValue(file, Workload.class));
            workloadFiles.put(name, file);
        }

        log.info("Workloads: {}", writer.writeValueAsString(workloads));

        Worker worker;

        if (arguments.workers != null && !arguments.workers.isEmpty()) {
            List<Worker> workers =
                    arguments.workers.stream().map(HttpWorkerClient::new).collect(toList());
            worker = new DistributedWorkersEnsemble(workers, arguments.extraConsumers);
        } else {
            // Use local worker implementation
            worker = new LocalWorker();
        }

        List<Throwable> failures = new ArrayList<>();
        try {
            for (Map.Entry<String, Workload> workloadEntry : workloads.entrySet()) {
                String workloadName = workloadEntry.getKey();
                Workload workload = workloadEntry.getValue();
                for (String driverConfig : arguments.drivers) {
                    File resultFile = null;
                    RunManifest manifest = null;
                    WorkloadGenerator generator = null;
                    try {
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
                        File driverConfigFile = new File(driverConfig);
                        DriverConfiguration driverConfiguration =
                                mapper.readValue(driverConfigFile, DriverConfiguration.class);
                        workload.validate();
                        validateRunConfiguration(driverConfiguration.run);
                        log.info(
                                "--------------- WORKLOAD : {} --- DRIVER : {}---------------",
                                workload.name,
                                driverConfiguration.name);

                        boolean useOutput = arguments.output != null && !arguments.output.isEmpty();
                        resultFile =
                                useOutput
                                        ? new File(arguments.output)
                                        : defaultResultFile(workloadName, driverConfiguration, dateFormat);
                        ensureParent(resultFile);
                        if (driverConfiguration.run != null) {
                            manifest =
                                    prepareRunManifest(
                                            resultFile,
                                            driverConfigFile,
                                            workloadFiles.get(workloadName),
                                            driverConfiguration,
                                            workload,
                                            worker,
                                            arguments.workers);
                            writeRunManifest(resultFile, manifest);
                        }

                        worker.stopAll();
                        worker.initializeDriver(driverConfigFile);

                        if (manifest != null) {
                            manifest.status = "RUNNING";
                            manifest.runtimeInfo = worker.getDriverRuntimeInfo();
                            manifest.workerId =
                                    manifest.runtimeInfo == null ? null : manifest.runtimeInfo.namespace;
                            manifest.measurementStartedAt = Instant.now().toString();
                            writeRunManifest(resultFile, manifest);
                        }

                        generator =
                                new WorkloadGenerator(
                                        driverConfiguration.name, driverConfiguration.run, workload, worker);
                        TestResult result = generator.run();
                        log.info("Writing test result into {}", resultFile);
                        writeJsonAtomically(resultFile, result);
                        if (manifest != null) {
                            manifest.status = "SUCCEEDED";
                            manifest.completedAt = Instant.now().toString();
                            manifest.endedAt = manifest.completedAt;
                            manifest.runtimeInfo = result.runtimeInfo;
                            manifest.workerId =
                                    manifest.runtimeInfo == null ? null : manifest.runtimeInfo.namespace;
                            manifest.samples = result.samples;
                            manifest.payloadMode = result.payloadMode;
                            manifest.payloadMessageSize = result.payloadMessageSize;
                            manifest.payloadRandomBytesRatio = result.payloadRandomBytesRatio;
                            manifest.payloadPoolSize = result.payloadPoolSize;
                            manifest.payloadSeed = result.payloadSeed;
                            manifest.payloadSha256 = result.payloadSha256;
                            manifest.assignmentSha256 = result.assignmentSha256;
                            manifest.requestedBacklogBytes = result.requestedBacklogBytes;
                            manifest.backlogAtDrainStartMessages = result.backlogAtDrainStartMessages;
                            manifest.backlogBuildDurationSeconds = result.backlogBuildDurationSeconds;
                            manifest.backlogDrainDurationSeconds = result.backlogDrainDurationSeconds;
                            manifest.averageDrainRateMessagesPerSecond = result.averageDrainRateMessagesPerSecond;
                            manifest.peakDrainRateMessagesPerSecond = result.peakDrainRateMessagesPerSecond;
                            manifest.postDrainBacklogMessages = result.postDrainBacklogMessages;
                            manifest.brokerBacklogAtDrainStartMessages = result.brokerBacklogAtDrainStartMessages;
                            manifest.brokerBacklogAfterDrainMessages = result.brokerBacklogAfterDrainMessages;
                            manifest.backlogPhase = result.backlogPhase;
                            writeRunManifest(resultFile, manifest);
                        }
                    } catch (Exception e) {
                        log.error(
                                "Failed to run the workload '{}' for driver '{}'", workload.name, driverConfig, e);
                        if (manifest != null) {
                            manifest.status = "FAILED";
                            manifest.completedAt = Instant.now().toString();
                            manifest.endedAt = manifest.completedAt;
                            manifest.failureType = e.getClass().getName();
                            manifest.failureMessage = safeFailureMessage(e);
                            try {
                                writeRunManifest(resultFile, manifest);
                            } catch (IOException manifestError) {
                                log.error("Failed to write failed-run manifest", manifestError);
                            }
                        }
                        failures.add(e);
                    } finally {
                        if (generator != null) {
                            try {
                                generator.close();
                            } catch (Exception e) {
                                log.warn("Failed to close workload generator", e);
                            }
                        }
                        worker.stopAll();
                    }
                }
            }
        } finally {
            worker.close();
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException(
                    "One or more benchmark runs failed; refusing a successful exit", failures.get(0));
        }
    }

    private static File defaultResultFile(
            String workloadName, DriverConfiguration driverConfiguration, DateFormat dateFormat) {
        if (driverConfiguration.run != null) {
            return new File(
                    "results/"
                            + driverConfiguration.run.campaignId
                            + "/"
                            + driverConfiguration.run.runId
                            + "/result.json");
        }
        return new File(
                String.format(
                        "%s-%s-%s.json",
                        workloadName, driverConfiguration.name, dateFormat.format(new Date())));
    }

    private static void ensureParent(File file) throws IOException {
        File parent = file.getAbsoluteFile().getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.isDirectory()) {
            throw new IOException("Cannot create result directory " + parent);
        }
    }

    private static RunManifest prepareRunManifest(
            File resultFile,
            File driverFile,
            File workloadFile,
            DriverConfiguration driverConfiguration,
            Workload workload,
            Worker worker,
            List<String> workerUrls)
            throws IOException {
        if (workloadFile == null || !workloadFile.isFile()) {
            throw new IOException("workload file is not readable: " + workloadFile);
        }
        File directory = resultFile.getAbsoluteFile().getParentFile();
        if (!driverFile.isFile()) {
            throw new IOException("driver file is not readable: " + driverFile);
        }
        Files.copy(
                driverFile.toPath(),
                new File(directory, "driver.yaml").toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                workloadFile.toPath(),
                new File(directory, "workload.yaml").toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        RunManifest manifest = new RunManifest();
        manifest.run = driverConfiguration.run;
        manifest.driverName = driverConfiguration.name;
        manifest.workloadName = workload.name;
        manifest.driverConfigPath = driverFile.getAbsolutePath();
        manifest.workloadPath = workloadFile.getAbsolutePath();
        manifest.driverConfigSha256 = sha256(driverFile);
        manifest.workloadSha256 = sha256(workloadFile);
        manifest.startedAt = Instant.now().toString();
        manifest.resultPath = resultFile.getName();
        populateEnvironmentEvidence(manifest, worker, workerUrls);
        return manifest;
    }

    private static void populateEnvironmentEvidence(
            RunManifest manifest, Worker worker, List<String> workerUrls) throws IOException {
        if (workerUrls == null || workerUrls.isEmpty()) {
            manifest.workerCount = 1;
            manifest.workerUrls.add(worker.id());
            manifest.workerRoleAssignment.put(worker.id(), "local");
        } else {
            manifest.workerUrls = new ArrayList<>(workerUrls);
            manifest.workerCount = workerUrls.size();
            if (worker instanceof DistributedWorkersEnsemble) {
                manifest.workerRoleAssignment.putAll(
                        ((DistributedWorkersEnsemble) worker).getWorkerRoleAssignment());
            } else {
                for (String workerUrl : workerUrls) {
                    manifest.workerRoleAssignment.put(workerUrl, "worker");
                }
            }
        }

        manifest.ombGitSha = firstEnvironment("OMB_GIT_SHA", "SOURCE_SHA");
        manifest.ombImage = firstEnvironment("OMB_IMAGE", "OMB_IMAGE_REF");
        manifest.ombImageDigest = firstEnvironment("OMB_IMAGE_DIGEST");
        manifest.ombRuntimeConfigId = firstEnvironment("OMB_RUNTIME_CONFIG_ID");
        manifest.kubernetesContext = firstEnvironment("OMB_KUBERNETES_CONTEXT");
        manifest.kubernetesNamespace = firstEnvironment("OMB_KUBERNETES_NAMESPACE");
        manifest.pulsarRelease = firstEnvironment("OMB_PULSAR_RELEASE");
        manifest.pulsarCluster = firstEnvironment("OMB_PULSAR_CLUSTER");
        manifest.pulsarStage = firstEnvironment("OMB_PULSAR_STAGE");
        manifest.brokerImageId = firstEnvironment("OMB_PULSAR_BROKER_IMAGE_ID");
        // Stage A deliberately exports an empty Nereus identity because it runs
        // the Apache baseline broker. Preserve that distinction from an unset
        // environment variable so the manifest records an empty string rather
        // than JSON null.
        manifest.nereusSourceIdentity = System.getenv("OMB_NEREUS_SOURCE_ID");

        String deploymentRunEnv = firstEnvironment("OMB_DEPLOYMENT_RUN_ENV", "NEREUS_RUN_ENV");
        if (deploymentRunEnv != null) {
            manifest.deploymentRunEnvPath = deploymentRunEnv;
            manifest.deploymentRunEnvSha256 = firstEnvironment("OMB_DEPLOYMENT_RUN_ENV_SHA256");
            if (manifest.deploymentRunEnvSha256 == null) {
                File runEnvFile = new File(deploymentRunEnv);
                if (runEnvFile.isFile()) {
                    manifest.deploymentRunEnvSha256 = sha256(runEnvFile);
                }
            }
        }
        manifest.helmEvidenceArchivePath =
                firstEnvironment("OMB_HELM_EVIDENCE_ARCHIVE", "NEREUS_HELM_EVIDENCE_ARCHIVE");
        manifest.helmEvidenceArchiveSha256 = firstEnvironment("OMB_HELM_EVIDENCE_ARCHIVE_SHA256");
        if (manifest.helmEvidenceArchiveSha256 == null && manifest.helmEvidenceArchivePath != null) {
            File evidence = new File(manifest.helmEvidenceArchivePath);
            if (evidence.isFile()) {
                manifest.helmEvidenceArchiveSha256 = sha256(evidence);
            }
        }
    }

    private static String firstEnvironment(String... names) {
        for (String name : names) {
            String value = System.getenv(name);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return null;
    }

    private static void writeRunManifest(File resultFile, RunManifest manifest) throws IOException {
        writeJsonAtomically(
                new File(resultFile.getAbsoluteFile().getParentFile(), "manifest.json"), manifest);
    }

    private static void writeJsonAtomically(File target, Object value) throws IOException {
        File directory = target.getAbsoluteFile().getParentFile();
        ensureParent(target);
        Path temporary = Files.createTempFile(directory.toPath(), target.getName(), ".tmp");
        try {
            writer.writeValue(temporary.toFile(), value);
            try {
                Files.move(
                        temporary,
                        target.toPath(),
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static String safeFailureMessage(Exception e) {
        String message = e.getMessage();
        if (message == null || message.isEmpty()) {
            return e.toString();
        }
        return message.length() <= 512 ? message : message.substring(0, 512);
    }

    private static String sha256(File file) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(Files.readAllBytes(file.toPath()));
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) {
                result.append(String.format("%02x", value & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("JVM does not provide SHA-256", e);
        }
    }

    private static void validateRunConfiguration(
            io.openmessaging.benchmark.driver.RunConfiguration run) {
        if (run == null) {
            return;
        }
        if (!run.isConfigured()) {
            throw new IllegalArgumentException(
                    "run requires campaignId, blockId, runId, stage, repetition and seed");
        }
        if (!run.stage.matches("[ABCDE]") || !run.runId.matches("[A-Za-z0-9._-]{1,128}")) {
            throw new IllegalArgumentException("run.stage or run.runId is invalid");
        }
    }

    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);
}
