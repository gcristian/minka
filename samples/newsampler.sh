#!/bin/bash
# ------------------- Minka Demo launcher --------------------
# scans for a non already used & busy port to configure minka 
# broker, starting at port 9000, takes the first one free
# ------------------------------------------------------------
port_start=9000
host='localhost'
xms='512M'
pp='broker.hostPort'
dsfp=$(pwd)'/'${1:-'datasets/mix.properties'}
another=false
for i in {0..20}; do
	echo $dsfp
	i=$(($i+$port_start))
	x=`netstat -ltn | grep $i | grep -v grep`
	y=`ps aux | grep "$pp=$host:$i"| grep -v 'grep'`
	if [ -z "$x" ] && [ -z "$y" ]; then
		echo "Using port: $i"
		cmd="mvn "
		[ "$another" ] && cmd=$cmd" compile install -DskipTests "
		p=$p"-DXms$xms -DXmx$xms exec:java "
		p=$p"-Dexec.mainClass=DatasetSampler.main "
		p=$p"-Dmins=1440 -D$pp=$host:$i "
		p=$p"-Ddataset.filepath=$dsfp"
		$cmd $p
		exit
	else
		another = true
		echo "Port: $i busy (netstat got $x)"
	fi
done

