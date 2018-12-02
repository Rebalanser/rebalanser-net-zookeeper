# rebalanser-net-zookeeper
ZooKeeper backend for .NET Rebalanser.

## Distributed Consensus of Resource Consumption Design
Rebalanser.ZooKeeper is responsible for detecting changes to live clients and available resources and ensuring that all participating clients share the available resources equally.

The design is split into two areas:
 - leader election
 - rebalancing after client or resources change.

Both processes can occur at the same time, though on termination of the leader election any in progress rebalancing is aborted, in order to start a new one with a fresh view on the members of the group.

The description below assumes knowledge of ZooKeeper concepts such as znodes and watches.

### Leader Election
When a client starts it registers an ephemeral sequential znode under the /group/clients path. ZooKeeper appends a monotonically increasing counter to the end of path that is unique to that client. This will create a path such as /chroot/group/clients/client_0000000007. 

If the client sees that it has the lowest sequence number then it designates itself the leader. The leader is called the "coordinator". If it sees that it is not the lowest, then it designates itself a follower. 

Once coordinator, the client writes to the /chroot/group/epoch znode in order to increment its version number.

Then the coordinator places the following watches:
 - getChildren() /chroot/group/clients
 - getChildren() /chroot/group/resources
 - getData() /chroot/group/epoch

The clients and resources watches tell the leader when it needs to perform rebalancing. The epoch watch will notify the leader when a different client also got elected leader. This causes the current leader to stop, close its session and start a new one as a follower.

Once a follower, a client sets a watch on the next lowest client znode. When a notification fires for that client it gets all children of the clients/ znode and if it has the smallest sequence number it is the coordinator, else it finds the next lowest znode, places a watch on it and continues to be a follower.

Followers place a watch on /chroot/group/status which will allow them to receive rebalancing commands from the coordinator via zookeeper.

![](https://github.com/Rebalanser/rebalanser-net-zookeeper/blob/master/images/znodes-and-watches.png)

![](https://github.com/Rebalanser/rebalanser-net-zookeeper/blob/master/images/coordinator-followers.png)

In addition to the coordinator detecting that another client was elected coordinator via an epoch notification, all writes to the status and resources znodes are performed with a known version. If another client has also written those znodes as coordinator then the version number will have been incremented and the write will fail. This ensures that zombie coordinators are unable to negatively impact rebalancing.

### Rebalancing
When a client comes or goes, or a resource is added/removed as a child znode to the /chroot/group/resources path, a rebalancing takes place.

The steps in a rebalancing are as follows:
1. Coordinator: A children notification from /chroot/group/clients or /chroot/group/resources is received.
2. Coordinator: SetData() "StopActivity" on  /chroot/group/status
3. Followers: Receive /chroot/group/status notification.
4. Followers: Invoke OnStop event handlers which call user code to stop consumption/writing of resources
5. Followers: create() ephemeral znode with ClientId (client_000000007) to /chroot/group/stopped
6. Coordinator: Receives /chroot/group/stopped notification and all ClientIds found in /chroot/group/clients match /chroot/group/stopped (excluding itself). GetChildren() /chroot/group/resources and /chroot/group/clients, then create key/value pairs in memory with resource-client assignments.
7. Coordinator: SetData() with resource assignments map on /chroot/group/resources
8. Coordinator: SetData() "ResourcesAssigned" on /chroot/group/status
9. Followers: Notification of /chroot/group/status "ResourcesAssigned"
10. Followers: GetData() on /chroot/group/resources to get resource assignment map.
11. Invoke OnStart event handler with resources matching its ClientId. This will execute user code to start consuming from/writing to these resources.
12. Followers: Delete() ephemeral child node of /chroot/group/stopped
13. Coordinator: Notifications received on /chroot/group/stopped.
14. Coordinator: When no more child znodes exist, call SetData() "StartConfirmed" on /chroot/group/status

![](https://github.com/Rebalanser/rebalanser-net-zookeeper/blob/master/images/rebalancing-zookeeper.png)

The coordinator or a follower could fail at anytime during rebalancing. The loss of the coordinator will cause leader election to be triggered. New coordinators always start a new rebalancing. When a new rebalancing is triggered, it aborts any in progress rebalancing.

Likewise, if any follower is lost midway through a rebalancing, the coordinator will detect that and abort the current rebalancing and start a new one.

To avoid disruptive rebalancing events during cluster start up and shutdown, rebalancing events are timed to occur with a minimum interval between them. This limits the number of rebalancings started and aborted during deployments.

