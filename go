#!/bin/bash
for i in {1..10}; do
	i=$(($i+9000))
	x=`netstat -ltn | grep $i`
	y=`ps aux | grep "brokerServerPort=$i"| grep -v 'grep'`
	if [ -z "$x" ] && [ -z "$y" ]; then
		echo "Using port: $i"
		mvn compile -o -DXms256M -DXmx512M test -pl server -Dtest=BootstrapTest -Dmins=1440 -DbrokerServerPort=$i
		exit
	else
		echo "Port: $i busy (netstat got $x)"
	fi
done

