# rebalanser-net-zookeeper
ZooKeeper backend for .NET Rebalanser (in development). Protocol and implementation still under development.

To learn more about the project check out the [wiki](https://github.com/Rebalanser/wiki/wiki).

## Internals
Rebalanser.ZooKeeper is .NET implementation of Rebalanser that uses ZooKeeper as the coordination service. 

The design is split into two areas:
 - leader election
 - rebalancing after client or resources change.

The description below assumes knowledge of ZooKeeper concepts such as znodes and watches.

Terminology:
- client: a participating node that is consuming (reading/writing) resources
- resource: a resource string identifier that clients use to identify the resources such as queues, S3 buckets, files, socket connections etc.
- coordinator: a client that is responsible for initiating rebalancing and assigning resources
- follower: a client that responds to rebalancing events to stop and start consumption of resources as dictated by the coordinator.
- resource group: a group of clients and resources
- chroot: the root ZooKeeper path that a resource group owns

### Resource Management
An administrator adds or removes resource identifiers to a resource group by creating/deleting child znodes of the /chroot/group/resources znode. This does not affect the version number of the /chroot/group/resources znode which would cause a BadVersion error for the coordinator during a rebalancing. 

### Leader Election
When a client starts it registers an ephemeral sequential znode under the /chroot/group/clients path. ZooKeeper appends a monotonically increasing counter to the end of path that is unique to that client. This will create a path such as /chroot/group/clients/client_0000000007. 

If the client sees that it has the lowest sequence number then it designates itself the leader. The leader is called the "coordinator". If it sees that it is not the lowest, then it designates itself a follower. 

Once coordinator, the client writes to the /chroot/group/epoch znode in order to increment its version number.

Then the coordinator places the following watches:
 - getChildren() /chroot/group/clients
 - getChildren() /chroot/group/resources
 - getData() /chroot/group/epoch

The clients and resources watches tell the leader when it needs to perform rebalancing. The epoch watch will notify the coordinator if a different client also got elected leader. This causes the current leader to stop, close its session and start a new one as a follower. This way we avoid two active coordinators.

Once a follower, a client sets a watch on the next lowest client znode. When a notification fires for that client it gets all children of the clients/ znode and if it has the smallest sequence number it is the coordinator, else it finds the next lowest znode, places a watch on it and continues to be a follower.

![](https://github.com/Rebalanser/rebalanser-net-zookeeper/blob/master/images/ResourceBarrier_LeaderElection.png)

In addition to the coordinator detecting that another client was elected coordinator via an epoch notification, all writes to znodes are performed with a known version. If another client has also written those znodes as coordinator then the version number will have been incremented and the write will fail. This ensures that zombie coordinators are unable to negatively impact rebalancing and will also detect their zombie state and revert to being followers.

### Rebalancing Algorithms
There are many possible algorithms and two are being implemented: Resource Barrier and Global Barrier. 

The Resource Barrier algorithm ensures that no resource can be consumed concurrently by two or more clients by placing barriers on individual resources. The pros of this algorithm are its simplicity and speed, and the negative is that it requires two RPCs per resource which may not be suitable for resource groups with hundreds to thousands of resources.

The Global Barrier algorithm ensures that no resource can be consumed concurrently by two or more clients by creating a start/stop command mechanism and a global barrier that ensures that all clients stop resource consumption before new resources are assigned. The pros of this algorithm are that it has the same number of RPCs regardless of the number of resources. The cons are that it is a more complex, slower algorithm (except for when there are hundreds to thousands of resources).

#### Resource Barrier Algorithm
Total RPCs. No of clients=C. No of resources=R.
- /clients: 1 GetChildren()
- /resources: 1 GetChildren(), 1 SetData(), 2xR SetData()

Overview of all RPCs to ZooKeeper.
![](https://github.com/Rebalanser/rebalanser-net-zookeeper/blob/master/images/ResourceBarrier_RPCs.png)

Followers place a data watch on /resources to detect when a new resource-client assignment has been made. When a client comes or goes, or a resource is added/removed as a child znode to the /chroot/group/resources path, a rebalancing takes place.

The steps in a rebalancing are as follows:
1. Coordinator: Receives a children notification from /chroot/group/clients or /chroot/group/resources and initiates a rebalancing. 
2. Coordinator: Gets the current list of resources
3. Coordinator: Gets the current list of clients
4. Coordinator: Creates a map of resource-client key values, equally sharing out resources between clients and performs a SetData() on /chroot/group/resources
5. Followers: Receive the /chroot/group/resources data notification
6. Followers: Invoke their OnStop event handlers
7. Followers: Perform a GetData() on /chroot/group/resources to get the resource assignments map
8. Followers: Foreach of their current resources they call Delete() on the resource barrier znode
9. Followers: Foreach newly assigned resource they call exists(watch) on /resources/resource-x/barrier. If the znode does not exist then a Create() ephemeral call is made to create the barrier znode. If the node exists then the prior owner has still not yet deleted it, as soon as the prior owner deletes it the new owner will receive the notification and add their own barrier. The new owner is blocked until they can place barriers on all their new resources.
10. Followers: The OnStart event handlers

Note that the coordinator also performs steps 6, 8, 9 and 10 as the coordinator also has resources assigned.

![](https://github.com/Rebalanser/rebalanser-net-zookeeper/blob/master/images/ResourceBarrier_Rebalancing.png)

This algorithm is asynchronous and the coordinator does not know when all followers have started consuming their resources. If any client (coordinator or follower) dies during the process then a new rebalancing will be started and the current one aborted. This was we avoid stuck rebalancings.

The following scenarios have been considered:
- When a follower receives a /chroot/group/resources data notification midway through a rebalancing they abort the current process and start from step 6.
- When a follower receives a session expired event they will create a new znode under clients. Add a data watch on /chroot/group/resources and wait for a new resource data notification (signalling a new rebalancing).
- When a leader receives any event that signals their loss of leadership (/epoch notification or a bad version on /chroot/group/resources or session expired) then they close their session (if open) and start a new one (most likely as a follower). This will trigger a new rebalancing due to another client becoming leader or the change in the /chroot/group/clients children znodes.

To avoid disruptive rebalancing events during cluster start up and shutdown or during leader changes, rebalancing events are timed to occur with a minimum interval between them. This limits the number of rebalancings started and aborted during deployments and leader changes.


#### Global Barrier Algorithm
Total RPCs. No of clients=C. No of resources=R. Number of stopped polling=P.
- /status znode: 3 SetData() and 3 notifications
- /clients: 1 GetChildren()
- /stopped: 2xC SetData(), P GetChildren()
- /resources: 1 GetChildren(), 1 SetData(), C GetData()

Followers place a watch on /chroot/group/status which will allow them to receive rebalancing commands from the coordinator via zookeeper.
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
14. Coordinator: When no more child znodes exist, logs the completion of the rebalancing

![](https://github.com/Rebalanser/rebalanser-net-zookeeper/blob/master/images/GlobalBarrier_Rebalancing.png)

The coordinator or a follower could fail at anytime during rebalancing. The loss of the coordinator will cause leader election to be triggered. New coordinators always start a new rebalancing. When a new rebalancing is triggered, it aborts any in progress rebalancing.

Likewise, if any follower is lost midway through a rebalancing, the coordinator will detect that and abort the current rebalancing and start a new one.

To avoid disruptive rebalancing events during cluster start up and shutdown, rebalancing events are timed to occur with a minimum interval between them. This limits the number of rebalancings started and aborted during deployments.

## Testing
Rebalanser has two invariants:
1. It will never ever assign a resource to more than one node 
2. No resource will remain unassigned longer than X period of time. 

The integration tests validate that invariant 1 holds no matter what and they test that invariant 2 holds given a minimum stability of the system. For example, I can overload the system, I can kill off parts of it, I can mess with the network, invariant 1 must always hold. Invariant 2 should hold as long as it is given sufficient stability where we should expect it to operate successfully. To test that I have a suite of integration tests that operate the distributed nodes in one process, with a physical ZooKeeper cluster. A second suite of tests not yet developed will deploy the nodes to multiple machines and stresses will be placed. Both invariants should hold and overall performance of the system should be reasonable.
