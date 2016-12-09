<center>![](https://k61.kn3.net/4/6/F/B/B/2/02D.png) </center>

### What is Minka ?
Minka is a Java library for distribution and balance of backend systems through sharding of user-defined duties. 
### What is it used for ?

Horizontally scale up usage of physical resources by splitting processing workload into application instances, and keeping a dynamic distribution and balancing of input signals.  

###Features
- Highly available service of distribution of duties and Resilient to failure
- Several weighted balancers to achieve flexible and continuous dynamic task distribution
- Mutipurpose abstract model with an Inversion of Control non-intrusive integration
- Isolation of scaling concerns out of the business layer by encapsulation

###Usage and backend model
|Duties|Pallets|Delegates|Shards|Leader|Followers|
|:-|:-|:-|:-|:-|:-|
|![](https://k61.kn3.net/F/C/A/5/9/C/6B5.png)|![](https://k60.kn3.net/C/4/E/3/4/F/4D6.png)|![](https://k60.kn3.net/0/A/F/E/2/B/008.png)|![](https://k61.kn3.net/1/6/6/8/0/5/58F.png)|![](https://k61.kn3.net/6/0/5/7/2/1/AA0.png)|![](https://k60.kn3.net/5/B/7/4/E/9/837.png)|
|user-defined distributable entities, representing input data or signal to user functions.|group duties, with specific balancing strategies based on lifecycle, costs and performance. |the API client applying the contract of reacting on distribution events sent to the shard|fragments of the duty universe, each machine in the cluster. | coordinates client CRUD requests. Controls everything.| receives duties and pallets from the leader, to invoke delegate's contract.

Check in detail:  http://gcristian.github.io/minka

###	Get in contact

For any doubts, improvements, contribution, write me to gcristian at gmail.

### License
Copyright © 2016 by Cristian Gonzalez 

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
```
http://www.apache.org/licenses/LICENSE-2.0
```
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.