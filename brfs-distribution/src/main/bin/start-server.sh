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

  BRFS_HOME=`cd "$PRGDIR/.." > /dev/null; pwd`

  echo $BRFS_HOME
fi

LIB_DIR="${DRUID_LIB_DIR:=${BRFS_HOME}/lib}"

case $1 in
  region)
    NODE_TYPE=regionnode
  ;;
  data)
    NODE_TYPE=datanode
  ;;
  init)
    java -Dbrfs.home=${BRFS_HOME} \
            -Dconfiguration.file=${BRFS_HOME}/config/regionnode/server.properties \
            -cp $LIB_DIR/*:${BRFS_HOME}/config/regionnode "com.bonree.brfs.server.Main" tools init
    echo "init process completed!"
    exit
  ;;
  restore)
    java -Dbrfs.home=${BRFS_HOME} \
            -Dconfiguration.file=${BRFS_HOME}/config/regionnode/server.properties \
            -cp $LIB_DIR/*:${BRFS_HOME}/config/regionnode "com.bonree.brfs.server.Main" tools restore
    echo "restore metadata completed!"
    exit
  ;;
  *)
    echo "usage: start-server.sh [region | node] [config_dir]"
    exit 1
  ;;
esac

###################配置文件信息########################
if [ $# -ge 2 ] && [ -d "$2" ]
then
  if [ -d "$2" ]
  then
    CONFIG_DIR_PARAM=$(cd "$2"; pwd)
  fi
fi

CONFIG_DIR="${CONFIG_DIR_PARAM:=${BRFS_HOME}/config/${NODE_TYPE}}"

# configuration file for server
CONFIG_FILE=${CONFIG_DIR}/server.properties

#日志配置文件
LOG_CONFIG=${CONFIG_DIR}/logback.xml
#程序日志输出路径
LOG_DIR=$BRFS_HOME/logs
if [ ! -d "$LOG_DIR" ]
then
  mkdir $LOG_DIR
fi

#Server ID配置路径
SERVER_ID_PATH=$BRFS_HOME/ids
if [ ! -d "$SERVER_ID_PATH" ]
then
  mkdir $SERVER_ID_PATH
fi

JVM_PARAMS=`sed -i 's/\r//' ${CONFIG_DIR}/jvm.config | grep -v "^#.*$" | cat`


#网络参数设置
DISK_NET_BACKLOG=2048
DISK_IO_THREADS=16
DUPLICATE_NET_BACKLOG=2048
DUPLICATE_IO_THREADS=16

cd ${BRFS_HOME}
case ${NODE_TYPE} in
		###启动副本管理###
		regionnode)
			nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dlog.dir=$LOG_DIR \
			-Dlog.file.name=${NODE_TYPE} \
			-Dconfiguration.file=${CONFIG_FILE} \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dnet.backlog=$DUPLICATE_NET_BACKLOG \
			-Dnet.io.threads=$DUPLICATE_IO_THREADS \
			-cp $LIB_DIR/*:${CONFIG_DIR} "com.bonree.brfs.server.Main" node region \
			> $BRFS_HOME/logs/regionnode.out 2>&1 &
			echo 'start region server completely!'
		;;
		###启动磁盘管理###
		datanode)
			nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dlog.dir=$LOG_DIR \
			-Dlog.file.name=${NODE_TYPE} \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dconfiguration.file=${CONFIG_FILE} \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dnet.backlog=$DISK_NET_BACKLOG \
			-Dnet.io.threads=$DISK_IO_THREADS \
			-cp $LIB_DIR/*:${CONFIG_DIR} "com.bonree.brfs.server.Main" node data \
			> $BRFS_HOME/logs/datanode.out 2>&1 &
			echo 'start disk server completely!'
		;;
		*)
		    echo "script error"
		    exit 1
		;;
esac
