#!/bin/sh

basedir=$(dirname `pwd`)
#程序jar全路径
SERVER=$basedir'/jar/FS_Server/FS_Server.jar' 

HEAPSIZE='-Xms512m -Xmx1024m'=
GC_PATH='-XX:+UseParNewGC -XX:+UseConcMarkSweepGC  -XX:+PrintGC -XX:CMSInitiatingOccupancyFraction=50 -XX:CMSMaxAbortablePrecleanTime=500 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:'$basedir'/logs/gc_server.log'

#资源管理lib路径
RESOURCE_LIB_PATH=$basedir'/lib'

#副本管理入口
DUPLICATION_MAIN_CLASS=com.bonree.brfs.duplication.BootStrap
#磁盘管理入口
DISK_MAIN_CLASS=com.bonree.brfs.server.ServerMain

case $1 in
		###启动副本管理###
		duplication)
			nohup java $HEAPSIZE $GC_PATH -Dpath=$basedir -Dresource_lib_path=$RESOURCE_LIB_PATH -cp $SERVER $DUPLICATION_MAIN_CLASS >/dev/null 2>&1 &
			echo 'Startup duplication server complete!'
		;;
		###启动磁盘管理###
		disk)
			nohup java $HEAPSIZE $GC_PATH -Dpath=$basedir -Dresource_lib_path=$RESOURCE_LIB_PATH -cp $SERVER $DISK_MAIN_CLASS >/dev/null 2>&1 &
			echo 'Startup disk server complete!'
		;;
		*)
			sh $0 disk
			sleep 2
			sh $0 duplication
		;;
esac
