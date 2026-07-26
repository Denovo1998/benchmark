# Apache Pulsar benchmarks

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
