#!/bin/sh

basedir=$(dirname `pwd`)
#程序jar全路径
SERVER=$basedir'/jar/FS_Server/FS_Server.jar' 

HEAPSIZE='-Xms512m -Xmx1024m'

GC_PATH='-XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:'$basedir'/logs/gc_server.log'

#资源管理lib路径
RESOURCE_LIB_PATH=$basedir'/lib'

if [ `jps | grep FS_Server.jar | wc -l` -gt 0 ]; then
   echo "warn: FS_Server is already running, please stop it first"
   exit -1
fi

nohup java -Dpath=$basedir -Dresource_lib_path=$RESOURCE_LIB_PATH -jar $HEAPSIZE $GC_PATH $SERVER >/dev/null 2>&1 &

echo -e "FS_Server has started.\nThe number of services running on this machine is `jps | grep FS_Server.jar | wc -l`."
