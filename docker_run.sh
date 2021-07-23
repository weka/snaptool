#!/bin/bash
config_dir=$PWD
auth_dir=$HOME/.weka/
time_zone=US/Eastern

if [[ -e /dev/log ]]; then syslog_mount='--mount type=bind,source=/dev/log,target=/dev/log'; fi
if [[ ! -f $config_dir/snaptool.yml ]]; then echo "'snaptool.yml' not found in '$config_dir'"; exit 1; fi
if [[ ! -f $config_dir/snap_intent_q.log ]]; then touch $config_dir/snap_intent_q.log; fi
if [[ ! -f $config_dir/snaptool.log ]]; then touch $config_dir/snaptool.log; fi

docker run -d --network='host' \
    -e TZ=$time_zone \
    $syslog_mount \
    --mount type=bind,source=$auth_dir,target=/weka/.weka/ \
    --mount type=bind,source=$config_dir,target=/weka \
    --mount type=bind,source=/etc/hosts,target=/etc/hosts \
    --name weka_snaptool \
    wekasolutions/snaptool -vv
