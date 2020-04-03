#!/bin/bash


case $1 in
		###启动副本管理###
		duplication)
			if [ `jps | grep "com.bonree.brfs.server.Main" | wc -l` -gt 0 ]; then
			    jps | awk '{if($4=="region")print $1}' | xargs kill
			    echo "duplication service has stopped."
			else
			    echo "Warn: duplication service is not running!"
			fi
		;;
		###启动磁盘管理###
		disk)
			if [ `jps | grep "com.bonree.brfs.server.Main" | wc -l` -gt 0 ]; then
			    jps | awk '{if($4=="data")print $1}' | xargs kill
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
