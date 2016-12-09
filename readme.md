<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://stackedit.io/res-min/themes/base.css" />
<script type="text/javascript" src="https://cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS_HTML"></script>
<script>e
  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
  })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');
  ga('create', 'UA-88637530-1', 'auto');
  ga('send', 'pageview');
</script>
</head>

<body><div class="container" width="70%"><p></p><center><img src="https://k61.kn3.net/4/6/F/B/B/2/02D.png" alt="" title=""> </center><p></p>

<h3 id="what-is-minka">What is Minka ?</h3>

<p>Minka is a Java library for distribution and balance of backend systems through sharding of user-defined duties. </p>



<h3 id="what-is-it-used-for">What is it used for ?</h3>

<p>Horizontally scale up usage of physical resources by splitting processing workload into application instances, and keeping a dynamic distribution and balancing of input signals.  </p>

<h3 id="features">Features</h3>

<ul>
<li>Highly available service of distribution of duties and Resilient to failure</li>
<li>Several weighted balancers to achieve flexible and continuous dynamic task distribution</li>
<li>Mutipurpose abstract model with an Inversion of Control non-intrusive integration</li>
<li>Isolation of scaling concerns out of the business layer by encapsulation</li>
</ul>

<h3 id="usage-and-backend-model">Usage and backend model</h3>

<table>
<thead>
<tr>
  <th align="left">Duties</th>
  <th align="left">Pallets</th>
  <th align="left">Delegates</th>
  <th align="left">Shards</th>
  <th align="left">Leader</th>
  <th align="left">Followers</th>
</tr>
</thead>
<tbody><tr>
  <td align="left"><img src="https://k61.kn3.net/F/C/A/5/9/C/6B5.png" alt="" title=""></td>
  <td align="left"><img src="https://k60.kn3.net/C/4/E/3/4/F/4D6.png" alt="" title=""></td>
  <td align="left"><img src="https://k60.kn3.net/0/A/F/E/2/B/008.png" alt="" title=""></td>
  <td align="left"><img src="https://k61.kn3.net/1/6/6/8/0/5/58F.png" alt="" title=""></td>
  <td align="left"><img src="https://k61.kn3.net/6/0/5/7/2/1/AA0.png" alt="" title=""></td>
  <td align="left"><img src="https://k60.kn3.net/5/B/7/4/E/9/837.png" alt="" title=""></td>
</tr>
<tr>
  <td align="left">user-defined distributable entities, representing input data or signal to user functions.</td>
  <td align="left">group duties, with specific balancing strategies based on lifecycle, costs and performance.</td>
  <td align="left">the API client applying the contract of reacting on distribution events sent to the shard</td>
  <td align="left">fragments of the duty universe, each machine in the cluster.</td>
  <td align="left">coordinates client CRUD requests. Controls everything.</td>
  <td align="left">receives duties and pallets from the leader, to invoke delegate’s contract.</td>
</tr>
</tbody></table>


<p>Check in detail:  <a href="http://gcristian.github.io/minka">http://gcristian.github.io/minka</a></p>

<h3 id="get-in-contact">Get in contact</h3>

<p>For any doubts, improvements, contribution, write me to gcristian at gmail.</p>



<h3 id="license">License</h3>

<p>Copyright © 2016 by Cristian Gonzalez </p>

<p>Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at</p>

<pre class="prettyprint prettyprinted"><code><span class="pln">http</span><span class="pun">:</span><span class="com">//www.apache.org/licenses/LICENSE-2.0</span></code></pre>

<p>Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.</p></div></body>
</html>