#!/bin/bash
start() {
	export HADOOP_HOME=/home/joseph/hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
	
	start-dfs.sh
	start-yarn.sh
}

stop() {
	export HADOOP_HOME=/home/joseph/hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
	
	stop-dfs.sh
	stop-yarn.sh
}

case $1 in
	start|stop) "$1" ;;
esac

exit
