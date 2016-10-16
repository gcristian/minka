![](https://k61.kn3.net/4/6/F/B/B/2/02D.png) 
###An abstract network sharding library
##Minka is a Java IoC model for distribution and balance of standalone  applications through sharding of user-defined duties

###Used to scale up resources by splitting processing workload into application instances, distribute input data to a cluster, and keep a dynamic balanced processing. 
####Strong efforts put on failure tolerance like network partitioning, and high availability thru transaction log and user-data replication, highly resilient and automatic interchangeable leader-follower shard roles, consistency of state, operations and data.</p>

*Not related to http requests, internet traffic load, routing, or DNS servers. 


##Basic concepts

####Usage
|Duties|Pallets|Delegate|
|:-|:-|:-|
|are user-defined distributable entities with an Id, payload and pallet. Typically representing input data to processes|allows grouping of similar duties, applying specific distribution and balancing strategies based on duty life-cycles, costs and performance. |the host app. implements a basic contract: it must take, release, and report their assigned duties, whenever it's commanded to. |

<table align="center"><tr><td align="center">
Fig 1: hierarchy and cardinality <br>
<img src="http://imgh.us/00_(16).svg" height="60%" width="60%"/>
</td></tr></table>


#### Backend
|Shards|Leader|Followers|
|:-|:-|:-|
|hold a fragment of the duty universe and cooperate for coordination with the other application instances needing to divide-and-conquer their input to scale usage of resources. | handles client requests on creation, read, update and delete operations. Controls follower processes pallet and duty migrations between shards, monitors | receives duties and pallets from the leader, to invoke delegate 

Minka receives `duties` from the client API endpoint, serialize, replicates, and transports them to `shards`, where they're taken by the application's `delegate`, to accomplish any purpose determined by the id, payload and `pallet`.

Allowing expansion of physical resource limitations like data storage, network bandwidth, cpu or memory,  by decoupling processes into granular distributable duties
#### 

----

### Integration Model

As a JAR file Minka runs embedded within the host application.
Once integrated Minka decouples the distribution problem into 3 phase-flow steps: 

1. *Reception*: of CRUD duty operations by the client API: involving storage consistency thru replication.
2. *Transport*: handling duty's life-cycle control flow, attaching, detaching, reallocating to online shards.
3. *Implementation*: your host app. taking responsibilities of given entities, to do whatever you need with them

<table align="center"><tr><td align="center">
Fig. 2: Decoupling distribution into a 3  phase-steps flow <br>
<img src="http://imgh.us/00_(17).svg" height="65%" width="90%"/>
</td></tr></table>
####Inversion of Control
The host aplication *yields lifecycle control of their duties to Minka*, and must implement a contract that implies to:

- receive updates on duties
- take responsibilities on duties
- release responsibilities on duties
- report given duties

After sending a CRUD request, and obtaining the required consistency-level response: client assumes those duties are available the application's instances to be attached or detached. 

####Duty Attributes

| name | type |  note |
|-|-|
| id | string | Max: 256 length for the identity, a domain unique value
| pallet | string | Max: 256  length, forthe pallet id 
| payload | bytes | serializable object representing the input data of a function 
|weight|long|the cost of performance of processing this duty at the shard. <br>Must be in the same unit as the shard's capacity weight for the related pallet.<br> It may or not be related with the payload|
| synthetic | boolean | default: false. If the duty must be present at all shards and never balanced.
| idempotent | boolean | default: true. If the duty can migrate for balancing purposes, i.e. be stopped and restarted at a different shard. <br>Otherwise is it called *stationary*.
| lazy | boolean | default: false. Determines the duty finalization. <br>If the duty abscense in delegate report means the duty ended and must not be re-attached. Otherwise client must send a deletion request.
|||

Note: synthetic duties cannot be lazy. Idempotency doesnt apply on synthetic duties. Typically all duties will be idempotent, and not synthetic.

Considering the life-cycle of a duty there are two types of duties: Idempotent or Stationary.

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
- a host JVM application
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

#Architecture
##Features
###Client request operations

Classic CRUD requests are enabled for Duties, with guarantees of Atomicity, Consistency and Durability.
The request is first sent to the leader so it can start a distributed transaction, with the other ISR leader candidates, replicating the leader's partition table.

Then the leader distribution process will select a Shard according the pallet it belongs to if it's a creation request, or the already assigned Shard where the duty is attached if it's an update or deletion.

> Replication in Minka is intended for transaction log redundancy, to ensure endurance of the operations requested.
This doesn't mean that Duties are available for the host application at several shards. **Duties are uniquely identified entities that Live in only one Shard at a time, with the exception of synthetic duties**`

| Minka Operation | Minka Client  API Rest | delegate contract method |  |
|-----------------|------------------------|--------------------------|--:|
| create | POST | attach |  |
| read | GET | report |  |
| update | PUT | notify |  |
| delete | DELETE | dettach |  |
| balancing | n/a | attach |  |
| balancing | n/a | dettach |  |
| monitoring | n/a | report |  |

####Duty payload
Duties have a payload part, that will be fetched from the client at the moment of creation, as a step in the transaction opened by the API. Such payload must be serializable in order to be transported to the Minka domain.

####Duty life-cycle
The life-cycle of a duty

- **begins** when a client API creation request is sent.
- **is attached** to an asigned shard for the first time
- for idempotent duties continues to be:
	- stopped and restarted when surviving migration caused by cluster re-balancing 
	- and so being **dettached** and **reattached** to shards,
- for stationary and idempotent duties: 
	- they're **notified** in case a client API update request is sent
	- they're **reattached** in case of fail tolerance policies.
- and then **finishes** when:
	- a client API deletion request is sent.
	- or for *lazy* duties: the delegate ceases to report it.


####Stationary duties
Such are the duties that can be distributed only once, and never balanced, because they arent idempotent and their functionality cannot be paused once started, for rebalancing purposes. May be for lacking of a distributed state persistance facility. 

###Distribution and balancing
There're four types of balancers that can be programatically or operationally, selected and dynamically changed, for pallets grouping duties, and each one can have its own balancer.

|Balancers |  |
|-|-|-|
|  *Round robin* <br><br> Classic algorithm, with the tweak that this balancer will always be keeping an even number of duties assigned to shards<br><br> Useful for very simple distribution, or when duties represent other than resource exhaustion <br><br> <img src="http://imgh.us/00_(12).svg" width="75%"/> <br><br>| *Spill over* <br><br> it fills a shard with all duties until reaching maximum capacity, then it accomodates new duties to other shards until it fulls, and so on. <br><br> Useful to incrementally grow usage of shards as they're fully filled, before reaching the next one in the cluster. <br><br> <img src="http://imgh.us/00_(11).svg" width="70%"/><br><br>
|*Capacity workload* <br><br> The most optimal algorithm, but considering the shards capacity for each pallet, and keeping a real balance of assignation. <br> Useful for elasticity when exhausting physical resources on very different machines. <br><Br><img src="http://imgh.us/00_(13).svg" width="75%"/>| *Fair workload* <br><br> Same pallets weight the same on different shards. <br>Duties are continuously weighted and redistributed to keep always all shards balanced, but ignoring the shard's capacity <br><br> <img src="http://imgh.us/00_(14).svg" width="70%"/>  |

####Fig 5. Sample of an application using all available balancers on different pallets
|pallet|size|balancer|note...|
|-|-|-|-|
|Green |26| *Capacity* | it can fit only in two shards. One shard responded zero allocation for this pallet.
|Pink |15| *Fair workload* |it's present in all shards with the same weight. |
|Blue |13|*Spill-over* | It filled the 1st shard now is coming for the 2nd. <br><br>
|Yellow|6| *Round-robin*|It's present in all shards with the same number of duties.
Then there's a White synthetic duty, it's only one but it's present in all shards.  
<table align="center"><tr><td align="center">Fig5. <br><br><img src="http://imgh.us/00_(10).svg" width="70%"/> |</td></tr><table>
 
 
<br><br>
####Duty weights
The weight is reported by the Delegate. It must be calculated once and then change only in case of need. If the weight changes: a rebalance may occurr, and the duty may migrate among shards.
> It's recommented the estimation to be more safe than accurate, and free of any factor subject to realtime variation, to avoid continuous redistribution by the balancers, causing a counter-productive effect on short-term duties.


####Synthetic duties
These are in rarely cases needed considering the best usage of Minka, but there're use cases where an application may need duties always distributed to all the shards where Minka runs (i.e. where the host application runs), that doesnt require the client to send as many creation requests as shards are in the cluster. 
For that matter, a duty creation request can be marked as *synthetic* and that will translate to a duty that it is never balanced, it will always live and be attached: in all shards. All CRUD operations apply, as if it were only 1 duty.

This's facility to provide distribution without balancing, to conduct input data to functions for applications always willing to run them.
> If this's the only feature used by the client, then Minka is not the right framework of choice.

##Design

<table align="center"><tr><td align="center">Fig5. <br><br><img src="http://imgh.us/00_(18).svg" width="80%"/> </td></tr><table>

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

The leader election relies on Zookeeper, thru apache curator framework. When a leader fails, other ISR shard becomes the new leader. 
###Communication
 Minka has a TCP socket event broker to asynchronously interchange messages between shards, built with Netty.
 Although brokers are directly connected, they dont talk, i.e., broker clients don't wait for an answer and broker servers don't produce it. They both serve a contract, staying functionally asynchronous for fluid though slow-paced orchestration, leveraging damage contention.
Only a TCP port is needed to which brokers connect each other, 
 
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
Condition of a shard keeping up the partition table, waiting for a leader candidate to fall, in order to enter competition for candidate with the most up-to-date partition table, avoiding downtime caused by a leader candidate shutdown.

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

## Scope and focus 

Minka may be related somewhat with other greatly known distributed solutions like Spark, Flume, Drill, Kafka, Storm, but only in the way that it could be an organ of them, because it's simpler and more abstract in its working model, and as it lacks of any practical orientation: it's also reduced in complexity and features.

Minka is not a database or filesystem

- it doesnt gives any facility to model data structures or articulate data flows, far from being related to.
- Although it supports data persistance and replication for high availability of service, and it can be used to store information.

Minka is not a processing framework

- it doesnt executes processes, nor gives synchronization primitives for coding, it doesnt handle threads
- But it allows to articulate an entity assignation policy, that can be used to distribute input data.

Minka is not a deployment or containerization tool

- it ignores the relation between the host application and the hardware where its held
- Although it supports hot topology changes, multitenancy, failure resilience.

We also believe that using Minka, an application can horizontally scale, becoming shards of its own cluster, while bringing also isolation of scaling concerns out of the business layer by encapsulation, with a simple IoC multi-purpose integration model.

### About the project and author

I started Minka as a hobbie and got stick to it after initial production testings.

I wrote the 90% of codebase after-hour, in less than 5 weeks, and the 10% in the following 4 months. It's a small system under 15K LOC. The project remained dormant until it stabilized for little production usage.

Today it's used in the company I work for: Flowics, allowing distribution of fetching schedules that feed our intake processing pipeline, whose main concern is to serve a social activity amplification SaaS. There we use a parafarnalia of technology like Kafka, Storm, Rabbit, Mongo, Elasticsearch, Postgres, we build our own artifacts, try not to reinvent the wheel, but sometimes there are no simple known solutions.

I believe it can be used for average critical achievements. For heavier usage it may require further unit testing.

I focused on keeping a very comprehensive model and code implementation, for what it is complex problem that it's easy to isolate of the business layer, with common sense in balancing algorithms.
