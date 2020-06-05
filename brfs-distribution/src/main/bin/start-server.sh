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
  update)
    if [ -z $2 ]; then
      echo "ids file is empty"
      echo "usage: start-server.sh update [disknode_id_file absolute path]"
    else
      cd ${BRFS_HOME}
      java -Dbrfs.home=${BRFS_HOME} \
            -Dconfiguration.file=${BRFS_HOME}/config/datanode/server.properties \
            -Dlog.dir=$BRFS_HOME/logs \
			      -Dlog.file.name=datanode \
			      -Dlogback.configurationFile=${BRFS_HOME}/config/datanode/logback.xml \
            -cp $LIB_DIR/*:${BRFS_HOME}/config/datanode "com.bonree.brfs.server.Main" update ids -i $2 \
            -c ${BRFS_HOME}/config/datanode/server.properties
      echo "deploy v1 update completed!"
     fi
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

JVM_PARAMS=`sed 's/\r//' ${CONFIG_DIR}/jvm.config | grep -v "^#.*$" | tr "\n" " "`

#网络参数设置
DISK_NET_BACKLOG=2048
DISK_IO_THREADS=16
DUPLICATE_NET_BACKLOG=2048
DUPLICATE_IO_THREADS=16

PID_FILE=${CONFIG_DIR}/${NODE_TYPE}_PID
if [ -f "${PID_FILE}" ]
then
  PID=`cat ${PID_FILE}`
  if [ `jps | awk '{print $1}' | grep ${PID} | wc -l` -gt 0 ]
  then
    echo "process[${PID}] has been running as ${NODE_TYPE}"
    exit 1
  fi
fi

cd ${BRFS_HOME}
case ${NODE_TYPE} in
		###启动副本管理###
		regionnode)
			eval nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dlog.dir=$LOG_DIR \
			-Dlog.file.name=BRFS_${NODE_TYPE^^} \
			-Dconfiguration.file=${CONFIG_FILE} \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dnet.backlog=$DUPLICATE_NET_BACKLOG \
			-Dnet.io.threads=$DUPLICATE_IO_THREADS \
			-cp $LIB_DIR/*:${CONFIG_DIR} "com.bonree.brfs.server.Main" node region \
			> $BRFS_HOME/logs/regionnode.out 2>&1 &

      COUNTER=0
			while [ x$region_pid = 'x' ] && [ $COUNTER -lt 5 ]; do
			    region_pid=`jps -lm | awk '{if($4=="region")print $1}'`
			    if [ x$region_pid = 'x' ]; then
			      COUNTER='expr $COUNTER+1'
			      sleep 1
			    fi
			done

			if [ x$region_pid != 'x' ]; then
			  echo $region_pid > ${PID_FILE}
			  echo "region node starts with pid[$data_pid]!"
			fi

			echo 'region node starts ERROR!'
		;;
		###启动磁盘管理###
		datanode)
			eval nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dlog.dir=$LOG_DIR \
			-Dlog.file.name=BRFS_${NODE_TYPE^^} \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dconfiguration.file=${CONFIG_FILE} \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dnet.backlog=$DISK_NET_BACKLOG \
			-Dnet.io.threads=$DISK_IO_THREADS \
			-cp $LIB_DIR/*:${CONFIG_DIR} "com.bonree.brfs.server.Main" node data \
			> $BRFS_HOME/logs/datanode.out 2>&1 &

      COUNTER=0
			while [ x$data_pid = 'x' ] && [ $COUNTER -lt 5 ]; do
			    data_pid=`jps -lm | awk '{if($4=="data")print $1}'`
			    if [ x$data_pid = 'x' ]; then
			      COUNTER='expr $COUNTER+1'
			      sleep 1
			    fi
			done

			if [ x$data_pid != 'x' ]; then
			  echo $data_pid > ${PID_FILE}
			  echo "data node starts with pid[$data_pid]!"
			fi

      echo "data node starts ERROR!"
		;;
		*)
		    echo "script error"
		    exit 1
		;;
esac
