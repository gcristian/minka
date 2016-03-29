minka
===================
####  Distributing application processes with a balanced sharding strategy

What happens when you're exhausting some resource and need to split the heavy processing load into several machines ? 
* How do you do when you need to scale that UoW (Unit of Work, virtually any process) ?
	- may be you start writing/reading tasks into/from some distributed highly available storage like any modern RDBMS
	- sooner or later you'll need to coordinate those processes
	- then you'll change the storage for some form of Queue...
	- but then you'll be limited for "realtime" consumption of events
	- what if you always have the same tasks that you need to be running all day long ?
	- what if your input data causes very different processing loads, and end-up having very unbalanced nodes, 	or big bottlenecks when heavy tasks appear with the corresponding downtime
	- then your producer must know about that and execute some sort of sensus on your application's work load
	- and may be you'll end up having some sort of weighted processing queues for different requirements, 
	- so then you're needing again some kind of RDBMS/Queue with lot of flags and preconditions
	
OK you're doing an ad-hoc non-repeatable distributed processing solution..

 * Even better:
	- what if your processing workflow's stages are implemented on different platforms and languages ?
	- what if you need taskforce load balancing ?
	- what if you need to scale with fail-over, high availability ?
	- what if you need complete flexibility in your applicatio cluster ?
	- you better have a specific solution absorving all this problems !!

Minka allows you to centralize and distribute your application's processes as mere tasks that can be passed on to shards among the cluster, having both Minka and your application, bundled together within the JVM, or separated.

It only requires to implement a contract taking responsibility of receiving, executing and relasing tasks 
commanded by Minka

You define what a Task is, what its payload is (format, packaged binaries, strings, files, whatever)
How these tasks enter and exit the cluster, to fit your specific lifecycles
Minka will get it from the intake endpoint, to the right place where your application can actually process it.
