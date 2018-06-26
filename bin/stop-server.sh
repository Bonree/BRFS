#!/bin/bash


case $1 in
		###启动副本管理###
		duplication)
			if [ `jps | grep BootStrap | wc -l` -gt 0 ]; then
			    jps | awk '{if($2=="BootStrap")print $1}' | xargs kill -9
			    echo "duplication service has stopped."
			else
			    echo "Warn: duplication service is not running!"
			fi
		;;
		###启动磁盘管理###
		disk)
			if [ `jps | grep ServerMain | wc -l` -gt 0 ]; then
			    jps | awk '{if($2=="ServerMain")print $1}' | xargs kill -9
			    echo "disk service has stopped."
			else
			    echo "Warn: disk service is not running!"
			fi
		;;
		*)
			sh $0 disk
			sleep 2
			sh $0 duplication
		;;
esac