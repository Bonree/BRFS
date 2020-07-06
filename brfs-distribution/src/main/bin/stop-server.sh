#!/bin/bash


case $1 in
		###启动副本管理###
		region)
		  region_pid=`jps -lm | awk '{if($2 == "com.bonree.brfs.server.Main" && $4=="region")print $1}'`
			if [ x$region_pid != 'x' ]; then
			    kill $region_pid
			    echo "region node has stopped."
			else
			    echo "Warn: region node is not running!"
			fi
		;;
		###启动磁盘管理###
		data)
		  data_pid=`jps -lm | awk '{if($2 == "com.bonree.brfs.server.Main" && $4=="data")print $1}'`
			if [ x$data_pid != 'x' ]; then
			    kill $data_pid
			    echo "data node has stopped."
			else
			    echo "Warn: data node is not running!"
			fi
		;;
    gui)
      gui_pid=`jps -lm | awk '{if($2=="com.bonree.brfs.gui.server.Server")print $1}'`
			if [ x$gui_pid != 'x' ]; then
			    kill $gui_pid
			    echo "gui node has stopped."
			else
			    echo "Warn: gui node is not running!"
			fi
		;;
		*)
			sh $0 data
			sleep 2
			sh $0 region
		;;
esac
