#!/bin/bash
if [[ -e /dev/log ]] 
then 
   export syslog_mount='--mount type=bind,source=/dev/log,target=/dev/log'
fi
touch snap_intent_q.log
touch snaptool.log
docker run --network host \
    -e TZ=America/Los_Angeles \
    $syslog_mount \
    --mount type=bind,source=/Users/bruceclagett/weka/.weka/,target=/weka/.weka/ \
    --mount type=bind,source=/etc/hosts,target=/etc/hosts \
    --mount type=bind,source=$PWD/snaptool.yml,target=/weka/snaptool.yml \
    --mount type=bind,source=$PWD/snap_intent_q.log,target=/weka/snap_intent_q.log \
    --mount type=bind,source=$PWD/snaptool.log,target=/weka/snaptool.log \
    wekasolutions/snaptool -vv vweka1,vweka2,vweka3
