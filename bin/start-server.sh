#!/bin/sh --

if [ x$BRFS_HOME = "x" ]; then
  PRG="$0"

  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
      PRG="$link"
    else
      PRG=`dirname "$PRG"`/"$link"
    fi
  done

  PRGDIR=`dirname "$PRG"`

  BRFS_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`

  echo $BRFS_HOME
fi

#程序jar全路径
SERVER=$BRFS_HOME/jar/FS_Server/FS_Server.jar

###################配置文件信息########################
#日志配置文件
LOG_CONFIG=$BRFS_HOME/config/logback.xml
#Server程序配置文件
SERVER_CONFIG=$BRFS_HOME/config/server.properties
#Server ID配置路径
SERVER_ID_PATH=$BRFS_HOME/ids

JVM_PARAMS=`cat $BRFS_HOME/config/jvm.config`

#资源管理lib路径
RESOURCE_LIB_PATH=$BRFS_HOME/lib

#副本管理入口
DUPLICATION_MAIN_CLASS=com.bonree.brfs.duplication.BootStrap
#磁盘管理入口
DISK_MAIN_CLASS=com.bonree.brfs.server.ServerMain

case $1 in
		###启动副本管理###
		duplication)
			nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dconfiguration.file=$SERVER_CONFIG \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dresource_lib_path=$RESOURCE_LIB_PATH \
			-cp $SERVER \
			$DUPLICATION_MAIN_CLASS >/dev/null 2>&1 &
			echo 'Startup duplication server complete!'
		;;
		###启动磁盘管理###
		disk)
			nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dconfiguration.file=$SERVER_CONFIG \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dresource_lib_path=$RESOURCE_LIB_PATH \
			-cp $SERVER \
			$DISK_MAIN_CLASS >/dev/null 2>&1 &
			echo 'Startup disk server complete!'
		;;
		*)
			sh $0 disk
			sleep 2
			sh $0 duplication
		;;
esac
