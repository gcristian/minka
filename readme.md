<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>minka-readme-master</title>
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

<h3 id="minka-is-a-java-library-for-distribution-and-balance-of-backend-systems-through-sharding-of-user-defined-duties">Minka is a Java library for distribution and balance of backend systems through sharding of user-defined duties.</h3>



<h4 id="used-to-horizontally-scale-up-usage-of-physical-resources-by-splitting-processing-workload-into-application-instances-and-keeping-a-dynamic-distribution-and-balancing-of-input-signals-minka-brings-isolation-of-scaling-concerns-out-of-the-business-layer-by-encapsulation-with-a-simple-ioc-multi-purpose-integration-model">Used to horizontally scale up usage of physical resources by splitting processing workload into application instances, and keeping a dynamic distribution and balancing of input signals. Minka brings isolation of scaling concerns out of the business layer by encapsulation, with a simple IoC multi-purpose integration model.</h4>



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
</tbody></table></div></body>
</html>