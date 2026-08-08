# Apache Pulsar benchmarks

## Nereus v0.1.0 campaign

The formal Nereus campaign is pinned to the Apache Pulsar `5.0.0-M1` client/admin
API. Its driver YAML carries one `run` identity (`campaignId`, `blockId`, `runId`,
stage, repetition and seed); `runId` must equal `client.namespaceSuffix`. Stages
A/B use the `bookkeeper` managed-ledger storage class and C/D/E use `nereus`.
The driver creates a deterministic namespace, writes and reads back the namespace
identity and persistence policy, and publishes the effective values in
`manifest.json`. It compares persistence fields individually because the Apache
5.0.0-M1 `PersistencePolicies.equals` implementation is not safe for String
storage-class values.

Render and validate a run on the benchmark host:

```bash
scripts/nereus-benchmark/render-run-config.sh \
  C c1-rate-100000 block-01-stage-C-rep-01 1 202607250101 /tmp/driver.yaml
scripts/nereus-benchmark/validate-run-config.sh /tmp/driver.yaml
```

Use the deterministic workload templates under
`workloads/nereus-v0.1.0/`. Formal results are written below
`results/<campaignId>/<runId>/` with the copied YAML inputs, result samples and
runtime manifest. `run-case.sh` refuses to start a formal run unless the Helm
deployment evidence file `results/deploy/latest.env` (or an explicitly passed
`NEREUS_RUN_ENV`) matches the driver stage, cluster, namespace and Kubernetes
context. The OMB Helm chart can pin `imagePullPolicy: Never`, node selectors and
a results PVC; see
`deployment/kubernetes/helm/benchmark/values.yaml`.
Set `imageDigest: sha256:<64 hex digits>` in formal values; the chart then
renders `image@digest` and rejects malformed digests during template rendering.

Build an immutable OMB image in the Kubernetes containerd namespace and record
its OCI digest:

```bash
scripts/nereus-benchmark/build-omb-image.sh
```

To move that image to another containerd node, save the tar, SHA-256 sidecar and
digest environment file, then import them in namespace `k8s.io`:

```bash
scripts/nereus-benchmark/containerd-transfer-omb-image.sh save \
  nereus-benchmark/openmessaging-benchmark:pulsar-b<git-sha>-amd64 \
  /tmp/omb.tar
CONTAINERD_USE_SUDO=true scripts/nereus-benchmark/containerd-transfer-omb-image.sh load \
  /tmp/omb.tar /tmp/omb.tar.env
```

Run one formal case only after the Pulsar Helm release has passed its verify and
evidence steps:

```bash
scripts/nereus-benchmark/run-case.sh \
  /tmp/driver.yaml \
  workloads/nereus-v0.1.0/s1-smoke.yaml \
  /tmp/omb-workers.yaml
```

During consumer setup, the driver retries only Pulsar's
`TopicDoesNotExistException` for a bounded period. This covers the short
metadata-convergence window after an Oxia-backed partitioned topic is created;
other setup errors still fail the run immediately, and the retry delay is
outside the measured workload interval.

Formal workloads set both `warmupDrainTimeoutSeconds` and
`measurementDrainTimeoutSeconds`. The Pulsar listener tracks every
`acknowledgeAsync` operation from listener entry through future completion. At
the warm-up boundary the framework pauses producer loops and waits for producer
in-flight, delivery backlog, and ACK in-flight to reach zero; acknowledged and
received counts must match, and Pulsar admin must report zero subscription
backlog in two consecutive polls one second apart. Only then does it reset
counters and latency histograms, rebuild the rate limiter, and record
`measurementStartedAt`.

The same drain runs after producer pause at the end of measurement before
cumulative latency is collected. A send or ACK error, redelivery count,
unsupported ACK tracking, drain timeout, count mismatch, or broker backlog read
failure marks a formal run `INVALID`; producers remain paused until worker
shutdown. An invalid run must be archived and followed by a fresh OMB release
and Pulsar cold reset, because a failed producer future can still correspond to
an append whose asynchronous recovery later commits.

For R1, keep the benchmark sample interval at one second, delete the selected
owner broker with `inject-broker-crash.sh`, and pass the resulting event file to
the analyzer. The injector records deletion and replacement-Ready timestamps;
`R1_OWNER_MAP_BEFORE` and `R1_OWNER_MAP_AFTER` can point to the owner-map JSON
snapshots collected by the operator:

```bash
scripts/nereus-benchmark/inject-broker-crash.sh \
  nereus-broker-0 pulsar /tmp/r1/fault-events.jsonl
scripts/nereus-benchmark/analyze-run.py \
  results/v010-202607/block-01-stage-C-rep-01/result.json \
  --fault-events /tmp/r1/fault-events.jsonl
```

B1 records both the OMB counter estimate and the Pulsar admin API's summed
partition `msgBacklog` at the drain boundary. A formal Pulsar B1 run fails
closed if broker backlog cannot be read.

`run-c1-sweep.sh` runs exactly one explicit C1 candidate. This is intentional:
the outer deployment workflow must cold-reset and reinstall Pulsar between
candidates. Start with `5k, 10k, 15k, 20k, 25k, 30k, 40k, 50k` msg/s, then
extend by 1.5x only after 50k passes. Analyze each result with optional latency
limits:

```bash
scripts/nereus-benchmark/analyze-run.py \
  results/<campaign>/<run>/result.json \
  --max-publish-p50-ms 18.1 \
  --max-publish-p99-ms 38 \
  > results/<campaign>/<run>/analysis.json
```

After A and B each have at least three repetitions per candidate, select their
highest common passing rate:

```bash
scripts/nereus-benchmark/select-common-rate.py \
  results/<campaign>/*stage-{A,B}-*/analysis.json \
  --stages A,B \
  --min-repetitions 3 \
  --require-latency-gate
```

The analyzer's data-plane decision still requires external CPU, network,
broker, BookKeeper and disk evidence before a formal PASS.

For instructions on running the OpenMessaging benchmarks for Pulsar, see the [official documentation](http://openmessaging.cloud/docs/benchmarks/pulsar/).

## Delayed message benchmarks

This fork adds simple support for Pulsar broker-side delayed delivery. To run a delayed-message benchmark:

- Use one of the delayed driver configs, for example:
  - `driver-pulsar/pulsar-delayed-1s.yaml` (all produced messages are delivered 1 second later)
  - `driver-pulsar/pulsar-delayed-5s.yaml` (all produced messages are delivered 5 seconds later)
  - `driver-pulsar/pulsar-delayed-10s.yaml` (all produced messages are delivered 10 seconds later)
- Or set `producer.messageDelayMs` in any existing Pulsar driver YAML (e.g. `driver-pulsar/pulsar.yaml`) to a value greater than 0 for a fixed delay.
- To use a per-message random delay, set `producer.minMessageDelayMs` and `producer.maxMessageDelayMs` (e.g. `minMessageDelayMs: 1000`, `maxMessageDelayMs: 10000` for a random delay between 1s and 10s). When `maxMessageDelayMs > 0`, the driver will ignore `messageDelayMs` and use a random value in `[minMessageDelayMs, maxMessageDelayMs]` for each message.
- To mix delayed and non-delayed messages in a single workload, use `delayMessageRatio`:
  - `delayMessageRatio: 0.0` (default) means all messages are sent without broker-side delay.
  - `delayMessageRatio: 0.3` means ~30% of messages will be delayed (according to `messageDelayMs` or the random range) and the rest will be sent immediately.
  - `delayMessageRatio: 1.0` means all messages are delayed (equivalent to the original delayed-only workloads).
- Workload YAMLs can also override these settings per workload by setting `messageDelayMs`, `minMessageDelayMs`, `maxMessageDelayMs` and `delayMessageRatio`.
- Delayed delivery requires `consumer.subscriptionType: Shared` or `consumer.subscriptionType: Key_Shared`. The default `driver-pulsar/pulsar.yaml` uses `Failover`, and Pulsar dispatches delayed messages immediately for unsupported subscription types.
- Then choose a workload YAML (for example `workloads/pulsar-delayed-1-topic-16-partitions-1kb.yaml`) and run the benchmark as usual.

End-to-end latency metrics will include the configured delivery delay.

## Supplement to the official documentation

Before you run `ansible-playbook` with `terraform-inventory`, you must set the environment variable `TF_STATE`. i.e. the completed command should be:

```bash
TF_STATE=. ansible-playbook \
  --user ec2-user \
  --inventory `which terraform-inventory` \
  deploy.yaml
```

### Ansible variable files

The Ansible deployment script supports flexible configuration with a variable file, which is specified by `-e` option like:

```bash
TF_STATE=. ansible-playbook \
  --user ec2-user \
  --inventory `which terraform-inventory` \
  -e @extra_vars.yaml \
  deploy.yaml
```

For example, if you changed the AWS instance type, the two SSD device paths might not be `/dev/nvme1n1` and `/dev/nvme2n1`. In this case, you can configure them like

```yaml
disk_dev:
  - /path/to/disk1
  - /path/to/disk2
```

See more explanations in [the example variable file](./deploy/ssd/extra_vars.yaml).

### Enable protocol handlers

With the Ansible variable file, you can enable multiple protocol handlers in `protocol_handlers` variable. For example, given following configurations:

```yaml
protocol_handlers:
  - protocol: kafka
    conf: kop.conf
    url: https://github.com/streamnative/kop/releases/download/v2.9.2.5/pulsar-protocol-handler-kafka-2.9.2.5.nar
  - protocol: mqtt
    conf: mop.conf
    url: https://github.com/streamnative/mop/releases/download/v2.9.2.5/pulsar-protocol-handler-mqtt-2.9.2.5.nar
```

It will download KoP and MoP from the given URLs. Then, the configuration templates will be formatted and appended to the `broker.conf`. The `conf` field is the name of the configuration template, which must be put under `templates` directory.

### Restart the brokers with new configurations

You can change the configuration files and then restart the cluster by executing the following command.

```bash
TF_STATE=. ansible-playbook \
  --user ec2-user \
  --inventory `which terraform-inventory` \
  -e @extra_vars.yaml \
  restart-brokers.yaml
```

