#!/bin/sh

basedir=../

#程序jar全路径
SERVER=$basedir'jar/FS_Server.jar' 

#日志路径
LOG_DIR=$basedir'logs/fs_server'

HEAPSIZE='-Xms512m -Xmx1024m'

GC_PATH='-XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:'$basedir'logs/gc_server.log'

if [ `jps | grep FS_Server.jar | wc -l` -gt 0 ]; then
   echo "warn: FS_Server is already running, please stop it first"
   exit -1
fi

nohup java -Dlog_dir=$LOG_DIR -jar $HEAPSIZE $GC_PATH $SERVER >/dev/null 2>&1 &

echo -e "FS_Server has started.\nThe number of services running on this machine is `jps | grep FS_Server.jar | wc -l`."
