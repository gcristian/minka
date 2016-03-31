# **What is Minka ?**

### A tool to scale up applications, providing a highly-available and fault-tolerant service of distribution and balancing of processing workload.

##### Applying the sharding pattern to divide-and-conquer application's resources, it allows user-defined unit of works to be: grouped, distributed, transported, and assigned into shards.

### Concepts of the model

**Duties** represent anything you can fine-grain within an application, like tasks, data, processes.
They can pan from a static only-once execution to a dynamic idempotent nature, their lifecycles may freely vary.
The minimal granularity of a processing function whose resources you need to make available by distribution.
You define...
 - what they mean, how to group them into pallets
 - how they’re weighted, and what balancing strategy they need
 - how and when they enter and exit the cluster, fitting your lifecycle

**Shards** are the instances of your application running along with Minka, needing to divide-and-conquer their input to scale usage of resources. Internally a Minka shard consists of a follower process, and a leader candidate process, elected or in position to be.

**Delegates** are the client's implementations to receive duties from Minka, subjected to a basic contract: it must take, release, and report their assigned duties, whenever it's commanded to, this's mandatory for Minka to work

Everything set, Minka gets user duties from the intake endpoint to their corresponding application's delegate, in the right machine where it’s running, keeping the cluster balanced, and all duties assigned as long as there is at least one shard to do it.

![diagram](https://k60.kn3.net/A930AF8D2.png)
> in this case duties are provided by the application itself, this's all occurring on the same machine, 


### Features
- Distributed because the shards communicate thru HTTP ports, they can be anywhere as long as they can keep connected.
 - All actions over duties will be re-routed thru the leader and sent to the follower Shard 
- Highly available and fault tolerant because it runs roles of leader (coordinators) and followers (application managers) that react this way:
  - In case of server shutdown, hang-up, or leader in-communication or leader fail, their held duties will be automatically re-distributed to alive shards,
  - and the out of sync shard will also release all held duties, in order to avoid concurrency while waiting to communicate with the leader.
  - In case the failing server is also the leader of the ensemble, other shards will result elected.
- Agnostic because minka doesn’t take part into the behaviour or platform of the application's delegate.

It's a simple design focused on simple achievements.

### Implementation and requirements
- an Apache Zookeeper ensemble,
- two configurable HTTP ports, serving client requests and talking to other cluster shards,
- Java 8

Currently it’s a library running within the same application’s JVM.
In next releases it will turn to a rest java standalone application, enabling support to other languages and platforms.

##### Check out the classes and interfases to integrate minka:
 - the most of important of all: the [Partition Delegate](https://github.com/gcristian/minka/blob/master/server/src/main/java/io/tilt/minka/api/PartitionDelegate.java)
 - the distributed unit of work to be your [Duty](https://github.com/gcristian/minka/blob/master/server/src/main/java/io/tilt/minka/api/PlainDuty.java)
 - the point of integration to run CRUD operations over duties: the [Partition Service](https://github.com/gcristian/minka/blob/master/server/src/main/java/io/tilt/minka/api/PartitionService.java)

##### How to test it
 - first you need to install and Apache Zookeeper, this should work for Ubuntu 14.04+
 - by default Minka expects ZK default address/port = localhost:2181, let it there
```
sudo apt-get install zookeeper
```
 - from the root minka folder compile the system using maven:
```
mvn compile
```
 - then execute these lines from different linux terminals: (2 or 3, take care of your memory):
```
./test 9000
./test 9001
./test 9002
```
 - they will all run a follower role, and candidate for a leader role.
 - there's also a demo Partition Delegate which will only print the duties that has been assigned to keep
 - all leader and follower roles behaviour is being logged at
```
/tmp/minka-leader.log
/tmp/minka-follower.log
/tmp/minka-other.log
```

### so what happens ?

- Minka starts with a demo instance of a PartitionDelegate that
 - returns the master list of duties when asked (at leader election)
 - saves assigned duties into a memory map,
 - and prints them if any assignation change occurs 
 - the demo Duty is simply a String ID value from "1" to "20"

![bootup](https://k61.kn3.net/57A5CC710.png)

- then each minka process
	- listens in a different HTTP port (9000,9001,9002), and connects to Zookeeper (2181)
	- starts a follower role that's waiting the aparition of a leader 
	- starts a leader candidate, but only one process won the election
	- every follower acknowleged the leader and started sending heartbeats
	- each heartbeat has a list of duties that the demo client is running

![folloers](https://k61.kn3.net/213E4AB77.png)

- and the elected leader
	- will ask the PartitionDelegate for the master duties 
	- will distribute them among the followers
	- will keep receiving heartbeats, maintaining a ranking of behaviour
	- will go on checking follower behaviour, duty balance, and prepared to react on any change

![](https://k60.kn3.net/7D15B90C3.png)


### Cool, but what if....

- the leader falls ?
	- another process will took the role immediately
- the follower falls ?
 - the leader will redistribute its duties keeping balance
- the follower flaps between a healthy and a sick state ?
 - according parametried values, it will be shutdown or be tolerated
- no leader is elected ? 
 - all followers will release duties, all leader candidates will retry their postulation
- communication is broken ?
 - all followers will release duties, leader's follower role will catch all duties
- communication flaps between healthly and sick ?
 - depending config, it will be tolerated or stop and keep retrying
- zookeeper falls ?
 - all followers will release duties, the leader will stop, both roles will retry connection forever
- when the leader, communication, or zookeeper comes back ?
 - everything will "turn to normality"
- I correctly shutdown a machine ?
	- the follower will say bye to the leader, and it wont take a sick verification time for their duties to be redistributed
	- the leader will pass on its data to the next leader elected on a different machine

The system is built with strong efforts on resilience, so it wont give up easily to work the right way.

---

### How do we all really start coding this ?

What happens when you're exhausting some resource and need to split the heavy processing load into several machines ?

 - may be you start writing tasks into some DB from where you’ll be taking tasks from.
 - and sooner or later you'll need to coordinate those poller processes
 - then you'll change the storage for some sort of queue…
  - So you'll be limited for "realtime" consumption of events
 - what if you always have the same tasks that you need to be running all day long ?
 - what if you don’t have tasks, but only data to be distributed ?
 - what if your input data causes very different processing loads ?
  - You’ll end-up having very unbalanced nodes, 
  - or big bottlenecks when heavy tasks appear with the corresponding downtime
 -  So your producer must prevent it by knowing some sort of sensus on your application's workload
 - Which may lead to have weighted processing queues for different requirements, 
 - so then you're needing again some kind of RDBMS/Queue with lot of flags and preconditions

OK you're doing an ad-hoc distributed processing solution, lot of complexity will keep arising from the first innocent initiative to solve the problem. And the What if’s could get funnier:

 - WIF you need to keep only data distributed not processes ?
 - WIF your processing stages are implemented on different platforms or languages ?
 - WIF you need to scale that with some guarantees of availability and failure tolerance ?
 - WIF you need complete flexibility in your application cluster ?

**You better be prepared :)**

## What it is not Minka

You cannot, should not

- Build Elasticsearch within Minka, but you could write similar interactions at a less stressing level.
- Balance web-application requests like you would with nGinx
- Run MapReduce, use Hadoop for that !

Minka does not necessary fit a big-data environment, it's more to distribution and balancing like ZK is to coordination, a tool, not a platform.

## Where it was born:

- in [Flowics]() we're a company for social media amplification, for which there's an ETL-like staging of info., before we can provide it to the frontend UI. The need arose to satisfy the distribution and balance of fetchers from multiple sources like Facebook, Instagram, Twitter, that required to be coordinated, as an elastic always available service.
 
I'm Cristian Gonzalez, I've been coding more time than the time I had not :) 

You can reach me at gcristian@gmail, and at: [Linkedin](https://www.linkedin.com/in/gcristian)
