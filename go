#!/bin/bash
# ------------------- Minka Demo launcher --------------------
# scans for a non already used & busy port to configure minka 
# broker, starting at port 8000, takes the first one free
# ------------------------------------------------------------

port_start=${1:-'8000'}
host='localhost'
xms='256M'
delegate=${1:-'MultiPalletSample'}
for i in {1..20}; do
	i=$(($i+$port_start))
	x=`netstat -ltn | grep $i`
	y=`ps aux | grep "brokerServerhost=$host:$i"| grep -v 'grep'`
	if [ -z "$x" ] && [ -z "$y" ]; then
		echo "Using port: $i"
		mvn compile -o -DXms$xms -DXmx$xms test -pl server -Dtest=CustomDelegateBootstrap \
			-Ddelegate=$delegate -Dmins=1440 -DbrokerServerHost=$host:$i
		exit
	else
		echo "Port: $i busy (netstat got $x)"
	fi
done

