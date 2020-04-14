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

CP=.
#程序jar全路径
for file in `ls $BRFS_HOME/lib`
do
  if [ -d $file ];then
    echo $file is directory will not add class path
  else
     CP=$CP:$BRFS_HOME/lib/$file
  fi
done

###################配置文件信息########################
#日志配置文件
LOG_CONFIG=$BRFS_HOME/config/logback.xml
#程序日志输出路径
LOG_DIR=$BRFS_HOME/logs

if [ ! -d "$LOG_DIR" ]
then
  mkdir $LOG_DIR
fi

LOG_DUPLICATE_OUT=$BRFS_HOME/logs/regionnode.out
LOG_DISK_OUT=$BRFS_HOME/logs/datanode.out
#Server程序配置文件
SERVER_CONFIG=$BRFS_HOME/config/server.properties
#Server ID配置路径
SERVER_ID_PATH=$BRFS_HOME/ids

if [ ! -d "$SERVER_ID_PATH" ]
then
  mkdir $SERVER_ID_PATH
fi

JVM_PARAMS=`grep -v "^#.*$" $BRFS_HOME/config/jvm.config | cat`

#资源管理lib路径
RESOURCE_LIB_PATH=$BRFS_HOME/native-lib

#网络参数设置
DISK_NET_BACKLOG=2048
DISK_IO_THREADS=16
DUPLICATE_NET_BACKLOG=2048
DUPLICATE_IO_THREADS=16

case $1 in
		###启动副本管理###
		region)
			nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dlog.dir=$LOG_DIR \
			-Dlog.file.name='regionnode' \
			-Dconfiguration.file=$SERVER_CONFIG \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dnet.backlog=$DUPLICATE_NET_BACKLOG \
			-Dnet.io.threads=$DUPLICATE_IO_THREADS \
			-Dresource_lib_path=$RESOURCE_LIB_PATH \
			-cp $CP "com.bonree.brfs.server.Main" node region \
			> $LOG_DUPLICATE_OUT 2>&1 &
			echo 'start region server completely!'
		;;
		###启动磁盘管理###
		data)
			nohup java $JVM_PARAMS \
			-Dbrfs.home=$BRFS_HOME \
			-Dlog.dir=$LOG_DIR \
			-Dlog.file.name='disknode' \
			-Dserver.ids=$SERVER_ID_PATH \
			-Dconfiguration.file=$SERVER_CONFIG \
			-Dlogback.configurationFile=$LOG_CONFIG \
			-Dnet.backlog=$DISK_NET_BACKLOG \
			-Dnet.io.threads=$DISK_IO_THREADS \
			-Dresource.lib.path=$RESOURCE_LIB_PATH \
			-cp $CP "com.bonree.brfs.server.Main" node data \
			> $LOG_DISK_OUT 2>&1 &
			echo 'start disk server completely!'
		;;
		init)
			java -Dbrfs.home=${BRFS_HOME} \
			-Dconfiguration.file=${SERVER_CONFIG} \
			-Dresource_lib_path=${RESOURCE_LIB_PATH} \
			-cp ${CP} "com.bonree.brfs.server.Main" tools init
			echo "init process completed!"
		;;
		*)
			sh $0 data
			sleep 2
			sh $0 region
		;;
esac
