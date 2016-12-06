<center>![](https://k61.kn3.net/4/6/F/B/B/2/02D.png) </center>
###Minka is a Java8 library for distribution and balance of backend systems through sharding of user-defined duties. 
####Used to horizontally scale up usage of physical resources by splitting processing workload into application instances, and keeping a dynamic distribution and balancing of input signals. Minka brings isolation of scaling concerns out of the business layer by encapsulation, with a simple IoC multi-purpose integration model.</h1>
###Usage and backend model
|Duties|Pallets|Delegates|Shards|Leader|Followers|
|:-|:-|:-|:-|:-|:-|
|![](https://k61.kn3.net/F/C/A/5/9/C/6B5.png)|![](https://k60.kn3.net/C/4/E/3/4/F/4D6.png)|![](https://k60.kn3.net/0/A/F/E/2/B/008.png)|![](https://k61.kn3.net/1/6/6/8/0/5/58F.png)|![](https://k61.kn3.net/6/0/5/7/2/1/AA0.png)|![](https://k60.kn3.net/5/B/7/4/E/9/837.png)|
|user-defined distributable entities, representing input data or signal to user functions.|group duties, with specific balancing strategies based on lifecycle, costs and performance. |the API client applying the contract of reacting on distribution events sent to the shard|fragments of the duty universe, each machine in the cluster. | coordinates client CRUD requests. Controls everything.| receives duties and pallets from the leader, to invoke delegate's contract.

###Implementation flow
Thru the *inversion of control* pattern, the host application yields to Minka the creation, update, deletion, shard selection,  and transportation of duties. The following three-steps flow takes place:
<table align="center"><tr><td align="center">
<br><img src="http://imgh.us/box-threstepsflow.svg" height="70%"/>
</td></tr></table>

###1. Integration
####1.1 Artifact

	<dependency>
		<artifactId>minka-parent</artifactId>
		<groupId>io.tilt.minka</groupId>
		<version>0.2-SNAPSHOT</version>
	</dependency>

####1.2 Requirements
- a Zookeeper cluster, used for leader election.
- a host JVM system, where the server runs embedded.

#### 1.2 Library

Minka provides a compact API at `io.tilt.minka.api` to do really few things:

- class `DutyBuilder/PalletBuilder` to wrap and internalize user entities
- class `MinkaClient` to input CRUD duties & pallets, after first balance.
- class `Minka` to start the server and listen for events.

#### 1.3 Events

Minka is designed to embrace incidents, that unfolds an event-driven architecture, which also applies its API. 
These are the events the client must listen and react to:

| event | required | when | what for |
|:-|-|-|-|-| 
|`load`|yes|balance *phase|input the initial duties and pallets to minka, only called at the first balancing phase of a new elected leader.| 
|`capture`, `release`|yes|distribution phase|honour the *Inversion of Control* contract about taking or discarding duties and pallets to perform a certain action, these are the core events.|
|`report`|yes|proctor phase|the captured duties, in permanent custody|
|`update`, `transfer`|no|anytime|reaction of an duty or pallet's payload update or a transferred message, to a duty or pallet previously *captured*. Entered thru the minka client|

*see the phases in the server implementation section.

####1.4 Sample
This creates a Minka server shard, starting with 1 pallet with a fair weight balancer, and 1 duty  weighing 200. When Minka yields control of the duty to us: we simply add it to *myDuties* set.
In a real use case all application instances must reuse the same routine and behave identically approaching Minka.

```java
final Pallet<String> p = PalletBuilder<String>
	builder("pal").with(Balancer.Strategy.FAIR_WEIGHT).build();
final Set<Duty<String>> myDuties = new TreeSet<>();

new Minka<String, String>("localhost:2181")
	.onPalletLoad(()-> newHashSet(p))
	.onDutyLoad(()-> newHashSet(DutyBuilder.<String>
		builder("aDuty", "pal").with(200d).build()))
	.onDutyCapture(duties->myDuties.addAll(duties))
	.onDutyRelease(duties->myDuties.removeAll(duties))
	.onDutyReport(()->myDuties)
	.setCapacity(p, 3000d)
	.load();
	// and we're distributed !
```
Every Minka class instance is a *Shard*, and can define which cluster is member of, by default is *unnamed*. In case we plan to share the zookeeper cluster with other minka-enabled applications: we must provide a custom name, or use a zookeeper's chroot folder. 

Every JVM should only have one member, only for testing we could specify different TCP ports, and under the same cluster name (*unnamed* is fine) or zk's chroot folder: we'll have a "standalone" cluster, communicating thru loopback interfase.

```java
final Config custom = new Config();
// if we're sharing ZK
custom.getBootstrapConf().setServiceName("sampleApp");
// if we're shooting several servers in the same machine
custom.getBrokerConfg().setHostPort("localhost:5749");
new Minka<String, String>(custom);
```

#### 1.5 Operations
> As in current version Minka lacks of a distributed-transactional storage, CRUD requests operate only in the current leader's lifecycle. At the event of a new leader reelection, all previous CRUD requests rely on their inclusion in the suppliers provided at the new elected leader, in the events `onPalletLoad` and `onDutyLoad`.

These are the CRUD requests that can be performed thru `io.tilt.minka.api.MinkaClient` class:

|method|action|
|--|--|
|add|adds a new pallet or duty to the system, in order for a shard to capture it and process it.|
|remove|remove an existing pallet or duty, for the shard that captured it: to release it.|
|update|update the pallet or duty payload at the shard held in custody|
|transfer|send a message to a pallet or duty, at the shard held in custody|

#### 1. 6 Sample 
This enters a newly created duty to the system, on the same existing pallet, and removes the previous one.

```java
MinkaClient<String, String> cli = server.getClient();
cli.add(DutyBuilder.<String>builder("other", "pal").build());
cli.remove(aDuty);
```

### 2. Limitations

- Minka lacks of a storage of its own in the \*current version, for duties, pallets and CRUD requests, the client must use a database to store input data, in case a new leader reelection occurrs (prepare for it),  because all elements beside those loaded at first: are held in memory by the leader itself. 
- every minka host application must behave identically, they run code that reacts to duties.
- host layer ignores distribution plumbing, states and environment, cannot rely on physical resources on the machine it runs.

What happens if your application is not subtled to process granularity ?

- Leaving out cases of bad modularization, scattered components or cluttered layers, big-coarsed grained applications arent good candidates for Minka integration, as we rely on it for external input of data or signals, and their balanced distribution for our own processing purposes. 

*see the roadmap

---


##3. Documentation
###3.1 Duties
A core and generic entity, wrapping anything transportable with significance only to the client. It might mean a signal for a process to start, input data for a function, a task name, a mesage, or simply data to be requested, uploaded, saved, analyzed, etc.
####3.1.1 Creation attributes

| name | type |  note |
|-|-|
| id | string | Max: 256 length, a unique identifier across the application instances.
| pallet id | string | Max: 256  length, required to link both entities inside the minka domain.
| payload| bytes | optional, serializable object representing input data or cargo for a function or process.
|weight|double|the cost of performance of processing this duty at a shard. Must be in the same measure unit as the weight reported for the pallet at the shards. It may or not be related with the payload|
|||

Considering the life-cycle of a duty there are two types of duties: Idempotent or *Stationary. (not supported in current version)*
For proper desired management, this's a concern to tell minka about your duties. Because Minka can attach or dettach them to/from shards in any moment, as there're many reasons subject but not limited to:

1. cluster health states variation
2. different new duty weights creates unbalanced cluster nodes, 
3. your application instances may be manually stopped or started, 
4. you add new shards and delete others,
5. network partitioning may occur, 
6. unexpected hardware or software failure, making the shard fall

All this conditions translate to duties being re-located, without impact if they represent static information having few dynamic impact. But if they represent some sort of input for a long-term job composed of ordered tasks, then your processes must be prepared for savepoints. This's due to its distributed resilient behaviour, and the multi-purpose nature of Minka, i.e. *what you do with it*. 

####3.1.2 Weights
The weight is reported by the Delegate. It must be calculated once and then change only in case of need. If the weight changes: a rebalance may occurr, and the duty may migrate among shards.

Table of duty weight samples:

|if a duty represents...| the weight might be measured | and shards reported capacity might reflect...|
|-|-|-|
|a file to save in harddisk|in bytes|the total harddisk space for that pallet|
|a file to upload at discretized locations |in a coefficient of bytes and network latency|the network uplink bandwidth with a coefficient of location in the network, that we want for that pallet|
|an image to analyze|in a coefficient between bytes and file format|might reflect a derivative of cpu clock speed and threads, that we want to assign to that pallet|

> It's recommented the estimation to be more safe than accurate, and free of any factor subject to realtime variation, to avoid continuous redistribution by the balancers, causing a counter-productive effect on short-term duties.

####3.1.3 Duty life-cycle
The lifecycle of a duty starts at creation and ends at removal. For both client requests to end up as attaching/dettaching events in a shard, among other events that also requires consistency management: there's a state flow that Minka follows:
<center><img src="http://imgh.us/box-dutylifecycle.svg" width="80%" /></center>

|name|type|desc|
|:-|-|:-|
|`create`, `remove`|client action|request sent thru `MinkaClient` methods that triggers minka events|
|`rebalance`|minka event|occurrs when the `Migrate` function effectively changes distribution |
|`attach`, `dettach`|minka event|translates to follower events: *capture* and *release*, caused by create, remove and rebalance|
|`prepared`|duty state|developed at balance phase, before being sent|
|`sent`|duty state|after the duty operation is transported to the follower, in pending state|
|`confirmed`|duty state|after the previous operation transport is confirmed by the follower|
|`dangling`|duty state|lost duty detected after dissapparition of a shard. Causes a rebalance.|
|`missing`|duty state|detected difference between partition table and a follower's heartbeat. Causes a rebalance.|

###3.2 Balancers

Balancers move duties from a source shard to a target shard with a functional criteria, by means of migration or creation. They have a fixed behaviour and continuously perform the same task no matter what cycle are in. There're four balancers that can be used for each individual pallet, at creation time thru:
```java
PalletBuilder<String>.builder("pal").with(Balancer.Strategy.FAIR_WEIGHT).build()
```

|Unweighted | Weighted |
|-|-|-|
| `Even size` (unweighted balanced)<br><br> function in the round-robin style, using a circular collection of shards, keeping an even number of duties among the shards. Useful when duties represent other than resource exhaustion <br><br> <img src="http://imgh.us/box-evensize.svg" height="60%"/> <br><br>| `Spill over` (weighted unbalanced)<br><br> function that fills a shard to its limits, before going to the next shard, and so on. Useful to incrementally grow usage of shards as they're really needed. <br><br> <img src="http://imgh.us/box-spillover.svg" height="60%"/><br><br>
|`Fair weight` (weighted balanced) <br><br> function that assigns -in terms of duty weight- a heavier  load to bigger reported capacity shards, and lighter load to lesser ones. Useful for elasticity or variable shard sizes. <br><br><img src="http://imgh.us/box-fairweight.svg" height="60%"/>| `Even weight` <br><br> function that keeps same weight load on the same pallets at different shards. Useful for duties representing coordination tasks or business logics <br><br> <img height="60%" src="http://imgh.us/box-evenweight.svg"/>  |

Sample of an application using all available balancers on different pallets
|pallet|size|balancer|note...|
|-|-|-|-|
|Green |26| *fair weight* | it can fit only in two shards. One shard responded zero allocation for this pallet.
|Red |15| *even weight* |it's present in all shards with the same weight. |
|Blue |13|*spillover* | It filled the 1st shard now is coming for the 3rd. <br><br>
|Purple|6| *even size*|It's present in all shards with the same number of duties.
Then there's a White synthetic duty (id'ed 55), it's only one but it's present in all shards.  
<center>![](http://imgh.us/box-allbalancers.svg)</center>

###3.3 Shards

Minka takes common known lessons on clustering like data replication, dynamically interchanging leader and follower roles, data sharding, asynchronous communication between nodes, etc. The following is an interaction diagram driven by its  main actors.

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

<center><img src="http://imgh.us/box-architecture_(1).svg" width="100%"/> </center>

###3.3 Communication
 Minka has a TCP socket event broker to asynchronously interchange messages between shards, built with Netty.
 Although brokers are directly connected, they dont talk, i.e., broker clients don't wait for an answer and broker servers don't produce it. They both serve a contract, staying functionally asynchronous for fluid though slow-paced orchestration, leveraging damage contention.
Only a TCP port is needed to which brokers connect each other.
 
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

*SPOF*
Minka has a single point of failure related to its dependency using Zookeeper (ZK) for leader election.
At the event of leadership election, that is the first time the cluster boots up, or when the leader have just went off, and a reelection takes place.
ZK must be available or all shards will release their duties an no entity will be assigned.
When a leader exists, ZK can fail without affecting Minka. If ZK and the current Leader fail at the same time: the Spof applies.

*Leader-Follower health rules*
If a follower ceases to receive or flaps the clearance from the Leader, which is a proof of communication, it releases its duties.
If a leader ceases to receive heartbeats from followers, their duties are re-attached to other followers, and the follower set in quarantine, until its beahaviour allows it to get back to the ensemble and receive duties.
If a follower flaps its heartbeats to the leader or like before they're unseen, health ranking goes down, and the shard is set to offline and the follower will lack of clearance, which will make the follower to release held duties. 

##4. Server implementation detail
The current section is for developers willing to add features to Minka. This detail is purely about server design from the leader process perspective.

When we create the server:
``` java
new Minka<String, Integer>("localhost:2181/minka");
```
Minka starts an embedded tcp server on port 5748, and communicates with other instances of the same host application: to organize distribution and balance of user entities.
<center>![](https://k60.kn3.net/C/D/2/7/F/C/6AC.png)</center>
The cluster enroles a Leader and its followers. The leader behaviour is composed of cyclic phases that work in order of hierarchy, and each one relies on the one before:
<center>![](https://k60.kn3.net/C/1/6/E/B/A/4CA.png)</center>

--- 
### 4.1 Bootstrap
When we integrate the library into our host application and call `load()`, triggers the Minka spring context, with all the starting agents.
``` java
new Minka<String, Integer>("localhost:2181/minka")
	.load();
```
<center>![](https://k61.kn3.net/C/6/1/9/4/2/E71.png)ba</center>
 
So the `bootstrap` resolves the current shard's host address and port, and starts the  `broker` that will be used for communication. It also starts a `scheduler` that control agents so they can safety run without interferring in other's work. Then starts both a `follower` and a `leader` process.
The Leader will register itself as a candidate at the `zookeeper` ensemble, if it wins it will start two agents: `proctor` and `distributor`, otherwise it will remain dormant until the last elected leader fails or shutsdown.
The Follower locates the current leader and thru the `heartpump` sends heartbeats to it. 
It will also start a `policies` agent to release duties in case the follower ceases to receive clearance from the leader.

### 4.2 Proctor 

This phase affects the supplier provided on the following events:
```java
final Set<Duty<String>> myCapture = new HashSet<>();
server.onDutyReport(()-> myCapture );
```

<center>
![](https://k60.kn3.net/C/5/6/E/7/D/F68.png)
</center>
The follower schedules the `heartpump` agent with a predefined frequency, which creates heartbeats containing information of the shard's identity, capacity, location, and attached duties reported by the user's delegate, and send them to the leader thru the `Broker`.
At the leader, all HB's passes to a `bookkeeper` function that analyzes the current cluster state, if there's a roadmap in progress, if it's the first HB of a shard, or anything of importance to account it. At first it will add a new Shard entity to the `partition table`.
When the `Proctor` agent starts, checks the partition table's shards and calculates a shard state involving the distance in time between the HBs and a predefined time-window range to rank up the shard, and move it to an ONLINE state, which will make after some periods in that state: to be considered for distribution of duties.

All shards starts in the JOINING state at the proctor's ranking, then go ONLINE, if there's some communication interruption strong enough it may cause the QUARANTINE status, and in case the shard orderly shutsdown goes to QUITTED, in case the shard ceases to send HBs or the Proctor ceases to see it: goes to OFFLINE.
<center>
![](https://k60.kn3.net/4/B/1/5/9/C/563.png)
</center>

The lasts states are urecoverables.
If there're shards in a state different than ONLINE: the phase doesnt moves ahead, but no shard is allowed to stay too much time in a temporal state like JOINING or QUARANTINE. 
When all shards are ONLINE after a number of predefined periods: the phase moves to balancing. In case there's a roadmap in progress, it goes on even if a shard is not ONLINE.

### 4.3 Balancer

<center><img src="http://imgh.us/box-balance.svg" width="35%" /> </center>

The following events occurrs when Minka needs to know the user entities at the first distribution, which happens only once in a leader's lifetime, and each time a new leader is reelected.
```java
ctx.onPalletLoad(()->...);
ctx.onDutyLoad(()->...);
```
<center>![](https://k60.kn3.net/B/3/1/0/A/2/B8F.png")</center>

The `distributor` agent checks the `cluster state` and if all shards are online it proceeds.
It asks the `partition master` (usually the partition delegate instance) for pallets and duties, which were defined thru `Minka::[onPalletLoad(..)|onDutyLoad(..)]`, this's the only moment that both domain entities are loaded and kept in partition table's custody thru adding them as a CRUD operation, the same one that can be executed thru `MinkaClient::add()`.
Then an `arrangement` function reads duties in the *Stage* (already distributed) and duties in the *NextStage* (CRUD ones), detects for missing duties (distributed but recorded as absent by the *Bookkeeper*), detects offline shards (and subsequently dangling duties to be redistributed).
The Arranger traverses all pallets with a `Balancer` predefined by the user thru the *BalancingMetadata* set at Pallet creation: with a *NextTable* composed of the Stage, the NextStage, and a `Migrator` that can be used to affect distribution process.
All provided Minka balancers will check for the NextTable's Shards and ShardEntities, and operate the Migrator for overriding shard's duties, transferring duties between shards, etc.
The Migrator validates every operation to be coherent and applies the changes to a `Roadmap` element that properly organizes them for the transportation process to be safety and smoothly applied.

###4.4 Distributor

<center><img src="http://imgh.us/box-distribution.svg" width="30%" /> </center>

this phase affects consumers defined on the following events:
```java
server.onDutyCapture((d)->...);
server.onPalletCapture((d)->...);
server.onDutyRelease((d)->...);
server.onPalletRelease((d)->...);
```
The `distributor` agent has the `roadmap` plan already built and starts driving it.
Thru the `broker` sends all duty detachments to their shard locations.
At the follower, the `partition manager` invokes the user's `partition delegate` to honor the duty contract: release the duties, stop them, kill them, whatever they mean for the host application.
So the `heartpump` continues to send heartbeats thru the broker: but without those released duties. 
At the leader, the `bookkeeper` notes the absence as a part of a roadmap plan, after ensuring this is a constant situation, it updates the `partition table` to reflect the new reality, and moves the roadmap pointer to the next phase: attachments. The same control flow applies for this step.
After both dettachments and attachments steps conclude, the distribution process starts again, after the proctor phase.
<center>![](https://k61.kn3.net/9/1/8/7/A/6/E79.png)</center>

##5. Project

###5.1 Scope
Minka is a simple and abstract model for shardig, balancing and distributing minimal data.
It's provided with no orientation, that's why is also reduced in complexity and features.
>Minka is not about routing http web requests for online realtime responses, but more about keeping a stable number of entities always balanced and available at a cluster of application instances. 

Minka is not a database or filesystem

- it doesnt gives any facility to model data structures or articulate data flows, far from being related to.
- Although it supports data persistance and replication for high availability of service, and it can be used to store information.

Minka is not a processing framework

- it doesnt executes processes, nor gives synchronization primitives for coding, it doesnt handle threads
- But it allows to articulate an entity assignation policy, that can be used to distribute input data or signals.

Minka is not a deployment or containerization tool

- it ignores the relation between the host application and the hardware where its held
- Although it supports hot topology changes, multitenancy, failure resilience.

###5.2 Roadmap

1. Release 0.1
	
	1.2. `done`: balancing and distribution flows
	1.2. `done`: initial leader's load of entities from DB and CRUD operations on duties
	1.3. `to-do`:  stationary and synthetic duties, for non-idempotent processes and coordination signals
	1.4. `to-do`: capture event marked with creation or migration, delegate might need it
	1.5. `to-do`: enforce unique ids validation across input methods.
	
2. Release 0.2
	4. `to-do`: enable usage of MinkaClient outside the JVM running the shard
	3. `to-do`: native storage for entities to support CRUD requests

### 5.3 About

I started Minka as a hobbie after-hour, nights became months. But it's still a baby framework under 15K LOC. 

Today it's used in the company I work for: [Flowics](https://github.com/gcristian/minka/blob/master), allowing distribution of fetching schedules that feed our intake processing pipeline, whose main concern is to serve a social activity amplification SaaS, on platforms: facebook, twitter, instagram, etc. There we use a parafarnalia of technology like Kafka, Storm, Rabbit, Mongo, Elasticsearch, Postgres, we build our platform, avoid reinventing the wheel, and innovate where there're no elegant or simple repeatable solutions yet.

I believe Minka can be used for average critical achievements. For more intensive or streaming usage, it may require further unit testing, among features as well.
I focused on keeping a very comprehensive model and code, for what it is complex problem that it's easy to isolate of the business layer, with common sense in balancing algorithms.

You can reach me at [linkedin](https://www.linkedin.com/in/gcristian) for any doubts. 

Cristian A. Gonzalez
Buenos Aires, 2016


