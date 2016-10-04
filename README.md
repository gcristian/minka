
# Minka 
##A network sharding framework 
###Minka is a distribution and balance library to scale-up standalone  applications through sharding of user-defined duties. Currently packaged as a Java IoC development framework. 

####Used to split physical resource work-load into application instances, distribute input data among shards of a cluster, and keep a dynamic balanced processing.

####Strong efforts put on failure tolerance like network partitioning, and high availability thru:
 
- transaction log and user-data replication
- automatic interchangeable leader-follower shard roles
- consistency of state, operations and data

## Basic concepts

***Duties*** are user-defined distributable entities with the following attributes:

- an Id to uniquely identify a duty in the cluster.
- a pallet to group similar duties, typically representing an application function.
- a payload, typically the pallet's function input data.

***Pallets*** is a facility to separate different duty types, applying specific distribution and balancing strategies based on duty life-cycles, costs and performance.

***Delegates*** are implemented by the host application, it applies a basic contract with Minka: it must take, release, and report their assigned duties, whenever it's commanded to. In case the duty represents the input of a task, the delegate will call the function, spawn a thread from a pool, or whatever it's meant to do with it.

***Shards*** hold a fragment of the duty universe and cooperate for coordination with the other application instances needing to divide-and-conquer their input to scale usage of resources. 

####Everything set, Minka receives `duties` from the client API endpoint, serialize, replicates, and transports them to `shards`, where they're taken by the application's `delegate`, to accomplish any purpose determined by the id, payload and `pallet`, 
####So expanding physical resource limitations like data storage, network bandwidth, cpu or memory,  by distribution of duties
#### 

----

### Integration Model

Minka decouples the distribution problem into 3 phase-flow steps: 

1. *Reception*: of CRUD duty operations by the client API: involving storage consistency thru replication.
2. *Transport*: handling duty's life-cycle control flow, attaching, detaching, reallocating to online shards.
3. *Implementation*: your host app. taking responsibilities of given entities, to do whatever you need with them

Assembly: Minka is a JAR, so your app. co-exists with it at the many instances, and thru it gets communicated to other nodes of the cluster.

####Inversion of Control
The host aplication yields lifecycle control of their duties to Minka, and must implement a contract whenever Minka commands it:

- receive updates on duties
- take responsibilities on duties
- release responsibilities on duties
- report given duties

And apply consistently in their own domain: CRUD operations over duties entering the cluster. 
So the host application receive external CRUD and translate them into duties so they can be balanced and distributed.
Your app. gives Minka total life-cycle control of your duties. You will create them, and Minka will get them saved and distributed to your application at a cluster node of its election, according configurations and health status.

After firing any CRUD operation on duties, and obtaining the required consistency-level response: you must assume that those duties will be available at your application's instances to be attached or detached. That's the base contract.

####Duty Attributes

*Idempotency*
For proper desired management, this's a concern to tell minka about your duties. Because Minka can attach or detach them to/from shards in any moment, as there're many reasons subject but not limited to:

1. cluster health states variation
2. different new duty weights creates unbalanced cluster nodes, 
3. your application instances may be manually stopped or started, 
4. you add new shards and delete others,
5. network partitioning may occur, 
6. unexpected hardware or software failure, making the shard fall

All this conditions translate to duties being re-assigned to shards, no problem if they represent static information having few dynamic impact. But if they represent some sort of input for a long-term job composed of ordered tasks, then your processes must be prepared for savepoints. This's due to its distributed resilient behaviour, and the multi-purpose nature of Minka, i.e. *what you do with it*. 

####Requirements
- a Zookeeper cluster 
- a Port
- a host application :)
#### Limitations

As a gold rule of Minka and any typical distributed system:

- every minka host application must behave identically, they run code that reacts to duty payloads
- host layer ignores distribution plumbing, states and environment,

As a clusterized host application, it cannot rely on any virtual or physical environment resource subject to HW variation. For example: you cannot save a file and expect to be always there, because your duty can be detached/attached from/to a different machine for the described reasons.

*Big-coarsed grained applications*

What happens if your application is not subtled to process granularity ?
Leaving out cases of bad modularization, scattered components or cluttered layers, big-coarsed grained applications arent good candidates for Minka integration, as we rely on Minka for external input of duties and their balanced distribution for our own processing purposes. 
BTW, a host app. that heavily relies on specific environment resources is in rare cases enable for clusterization. 



---

## Simpler, smaller 

It's not designed to solve the same use cases than big frameworks are like Storm, Spark, Flume, Drill., because it's simpler and more abstract in its working model, reduced in complexity, with less features.

Minka is not a deployment or containerization tool

- it ignores the relation between the host application and the hardware where its held
- Although it supports hot topology changes, multitenancy, failure resilience.

Minka is not a database or filesystem

- it doesnt gives any facility to model data structures or articulate data flows.
- Although it supports data persistance and replication.

Minka is not a coordination tool

- it doesnt gives synchronization primitives for coding, it doesnt handle threads, nor execute tasks
- But helps to distribute and balance external input 

#Architecture

It takes common known lessons on clustering like data replication, leader and follower roles, sharding, asynchronous communication between nodes, etc.

### Shards
They represent the union of the host application and the Minka library, which encompases these major ideas:

- a *follower* process that:
 - sends heartbeats to the leader, and receives clearance from it.
 - receives from the leader: actions on duties  to apply over the host application's *delegate*
- a *leader* process running or dormant candidate, that:
 - coordinates CRUD operations from the client API
 - distributes duties and keeps balance of the cluster
 - monitors followers for guaranteeing service policies
 - keeps up to date the leader's partition-table
 
####Resiliency

*Cluster member restart*
At the event of a clean restart, that is, the host application's JVM ordered shutdown.
If the shard has leadership on the cluster, any other member of the partition's table in-sync-replica will won leadership.
Duties held by the temporarily restarted member will be released before shutdown, and the leader will reattach them at other shards. This depends on the balancing rules of the pallets holding the duties.

*Cluster member partitioned*
At the event of a split of the ensemble, those still connected to the leader will keep working and 
those unseen by the leader will release their duties waiting for the leader's clearance. 
The leader will left those shards at the shadow and redistribute their duties into the members still inside the leader's subnet.
In case the leader is left alone at a subnet while all others still see each other, and all network partitions are still connected to a working zookeeper ensemble: the leader will consider itself as ghost and will decline leadership, provoking the followers still connected to zookeeper to reelect a new leader as usual.
In case the cluster is fully splitted, that is no shard has connection to others, no leader will trust stability.

####Consistency
*PTable*
The partition table holds a map with shards to duties, duty reallocation plans and states, and shard's heartbeats. This data is persisted into a local database uniquely by the current Leader of the cluster.

*ISR*
InSyncReplica, a common concept, applied to the Minka shards running the leader process or with a candidate in position to be, that keep-up the leader's partition table, mostly changing as a consequence of CRUD operations and cluster health states.

*Shadow ISR*
Condition of a shard keeping up the partition table, waiting for a leader candidate to fall, in order to enter competition for candidate with the most up-to-date partition table, in order to reduce downtime caused by a leader candidate shutdown.

*CRUD TX (transactional creation,read,update and deletion)*
When a Client fires a CRUD operation thru API, there starts an open distributed transaction among:

- the follower shard receiving the API call, 
- the current leader shard, 
- the candidate leader shards
- the shadow isr shards

the TX concludes when the configured consistency level is reached after the shards persist the operation on their ptable's local database.  

*SPOF*
Minka has a single point of failure related to its dependency using Zookeeper (ZK) for leader election.
At the event of leadership election, that is the first time the cluster boots up, or when the leader have just went off, and a reelection takes place.
ZK must be available or all shards will release their duties an no entity will be assigned.
When a leader exists, ZK can fail without affecting Minka. If ZK and the current Leader fail at the same time: the Spof applies.

*Leader-Follower health rules*
If a follower ceases to receive or flaps the clearance from the Leader, which is a proof of communication, it releases its duties.
If a leader ceases to receive heartbeats from followers, their duties are re-attached to other followers, and the follower set in quarantine, until its beahaviour allows it to get back to the ensemble and receive duties.
If a follower flaps its heartbeats to the leader or like before they're unseen, health ranking goes down, and the shard is set to offline and the follower will lack of clearance, which will make the follower to release held duties. 

###Minka also:
- enables applications to horizontally scale, becoming shards of their own cluster. 
- brings isolation of scaling concerns out of the business layer by encapsulation.
- manages data replication, transport, distribution, workload balancing, communication, self-monitoring.
- simple IoC multi-purpose integration model
- provides a client API for CRUD operations, and HTTP endpoints for sys-admin. 
