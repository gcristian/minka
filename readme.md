<center>![](https://k61.kn3.net/4/6/F/B/B/2/02D.png) </center>

###Minka is a Java library for distribution and balance of backend systems through sharding of user-defined duties. 
####Used to horizontally scale up usage of physical resources by splitting processing workload into application instances, and keeping a dynamic distribution and balancing of input signals. Minka brings isolation of scaling concerns out of the business layer by encapsulation, with a simple IoC multi-purpose integration model.</h1>
###Usage and backend model
|Duties|Pallets|Delegates|Shards|Leader|Followers|
|:-|:-|:-|:-|:-|:-|
|![](https://k61.kn3.net/F/C/A/5/9/C/6B5.png)|![](https://k60.kn3.net/C/4/E/3/4/F/4D6.png)|![](https://k60.kn3.net/0/A/F/E/2/B/008.png)|![](https://k61.kn3.net/1/6/6/8/0/5/58F.png)|![](https://k61.kn3.net/6/0/5/7/2/1/AA0.png)|![](https://k60.kn3.net/5/B/7/4/E/9/837.png)|
|user-defined distributable entities, representing input data or signal to user functions.|group duties, with specific balancing strategies based on lifecycle, costs and performance. |the API client applying the contract of reacting on distribution events sent to the shard|fragments of the duty universe, each machine in the cluster. | coordinates client CRUD requests. Controls everything.| receives duties and pallets from the leader, to invoke delegate's contract.
