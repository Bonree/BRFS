#!/bin/bash


case $1 in
		###启动副本管理###
		region)
			if [ `jps -lm | grep "com.bonree.brfs.server.Main" | wc -l` -gt 0 ]; then
			    jps -lm | awk '{if($4=="region")print $1}' | xargs kill
			    echo "region node has stopped."
			else
			    echo "Warn: region node is not running!"
			fi
		;;
		###启动磁盘管理###
		data)
			if [ `jps -lm | grep "com.bonree.brfs.server.Main" | wc -l` -gt 0 ]; then
			    jps -lm | awk '{if($4=="data")print $1}' | xargs kill
			    echo "data node has stopped."
			else
			    echo "Warn: data node is not running!"
			fi
		;;
		*)
			sh $0 data
			sleep 2
			sh $0 region
		;;
esac
