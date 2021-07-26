#!/bin/bash
# sample file for running snaptool as a docker container
# the wekasolutions/snaptool docker image can be downloaded from docker hub
#
config_dir=$PWD/logs
auth_dir=$HOME/.weka/
time_zone=US/Eastern

if [[ ! -f $config_dir/snaptool.yml ]]; then echo "'snaptool.yml' not found in '$config_dir'"; exit 1; fi

if [[ -e /dev/log ]]; then syslog_mount='--mount type=bind,source=/dev/log,target=/dev/log'; fi

docker run -d --network='host' --restart always \
    -e TZ=$time_zone \
    $syslog_mount \
    --mount type=bind,source=$auth_dir,target=/weka/.weka/ \
    --mount type=bind,source=$config_dir/snaptool.yml,target=/weka/snaptool.yml \
    --mount type=bind,source=$config_dir/logs,target=/weka/logs \
    --mount type=bind,source=/etc/hosts,target=/etc/hosts \
    --name weka_snaptool \
    wekasolutions/snaptool -vv
