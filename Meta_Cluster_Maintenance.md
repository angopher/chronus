# Meta Cluster Maintenance

## Get Status of Cluster

```shell
metad-ctl -s ip:port status
```

Sample output as:

```shell
Cluster:
Leader: 3
Term: 8
Committed: 4685619
Applied: 4685619

Nodes:
1      Follower       127.0.0.1:2345         StateReplicate 4685619=>4685620
2      Follower       127.0.0.1:2346         StateReplicate 4685619=>4685620    Vote(3)
3      Leader         127.0.0.1:2347         StateReplicate 4685619=>4685620    Vote(3)
```

## Restart Node

### Restart Follower

Feel free to restart any follower. The only thing you should take care is that
it's better to restart one node at a time and make sure the status of cluster
become healthy again.

### Restart Leader

Restart a leader should follow 2 phases:

1. Kill the leader
2. Check status of cluster whether a new leader has been elected
3. Start it and now it's a follower

## Add New Node

Adding operation should also follow 2 phases step.

1. Add it into the configuration using `metad-ctl add` specifying `id` and `addr`
2. Start the new, empty meta node

One node at a time. If you want to add multiple nodes just repeat the below steps.

## Remove Node

1. Kill it
2. Remove it from configuration using `metad-ctl remove`

## Replace Node

There are two strategies to replace an existing node.

First is remove-add strategy.
In this strategy you can remove it first follow steps in `Remove Node` and then
add a new node follow steps in `Add New Node`. The core point is that you can
use the same **address** / **id** of the removed one.

Another is add-remove strategy.
In this strategy you first add a new node into cluster and then remove the old
one. The core point is that it maybe safer compared to first strategy. But you
can't use the same id or address because they will both up for a while.

## Recover from Disaster

If something bad happened and the cluster wouldn't achieve consensus anymore or
there was other reason which caused cluster can't work anymore, here is how to get
them back.

First you should check which storage of node you want to recover with. Use commend
`metad -config <configuration> -dump a.db` to dump the storage to the file `a.db`.

Second you can boot up the first node using it through `metad -config <configuration> -restore a.db`.
Now you have a single-instance cluster. Then you can follow the `Add New Node` steps
to add the rest one by one.
