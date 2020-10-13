# Data Cluster Maintenance

## Get Status of Cluster

### Node List

Use following to list all data nodes in cluster (no matter alive or dead):

```shell
influxd-ctl -s ip:port node list
```

Where `ip:port` is any **TCP address** of **alive** node in this cluster.

Sample output as:

```shell
Nodes:
4    http://:8092    tcp://127.0.0.1:8082
5    http://:8093    tcp://127.0.0.1:8083
6    http://:8094    tcp://127.0.0.1:8084
7    http://:8095    tcp://127.0.0.1:8085
8    http://:8096    tcp://127.0.0.1:8086
9    http://:8091    tcp://127.0.0.1:8081
15    http://:8097    tcp://127.0.0.1:8087
```

### Shards on Node

Use following to list all available shards (only id) on specific node:

```shell
influxd-ctl -s ip:addr shard node <node-id>
```

Output:

```shell
Shards on node 15:
[513 549 556 575 578 580 582 585 593 594]
```

### Shards of Retention Policy

```shell
influxd-ctl -s ip:port shard list <database> <retention policy>
```

### Single Shard Info

```shell
influxd-ctl -s ip:port shard info <shard id>
```

Output:

```shell
Shard: 594
Database: _internal
Retention Policy: monitor
Nodes: [15]
```

## Restart Node

Feel free to restart any node if you have **handoff hinted**(hh) service enabled
on every other node. New replicated data blocked will be cached and retry to
replicate when the node was back online. Any failed query sent to this node will
be retried to other replicas.

## Add New Node

Adding operation is simple. Configure it and start it then it will appear in
node list.

## Remove Node

1. Remove it from configuration through `influxd-ctl node remove`
2. Stop the instance

## Replace Node

Replacement is more complicated. For instance we call the instance to be replaced
as `A` and the new one as `B`.

1. Add B into cluster
2. Freeze both A and B through `influxd-ctl node freeze`
3. Truncate shards and wait a while to make sure no further writes on A and B
4. Get all shards through `influxd-ctl shard node`
5. Copy them from A to B through `influxd-ctl shard copy`
6. Progress can be checked through `influxd-ctl shard status`
7. Better to verify the actual data directories are copied correctly
8. Remove A from cluster
9. Unfreeze B to let it accept creation of new shards
