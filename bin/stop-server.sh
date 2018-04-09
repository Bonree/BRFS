#!/bin/bash

if [ `jps | grep FS_Server.jar | wc -l` -gt 0 ]; then
    jps | awk '{if($2=="FS_Server.jar")print $1}' | xargs kill -9
    echo "FS_Server service has stopped."
else
    echo "Warn: FS_Server service is not running!"
fi