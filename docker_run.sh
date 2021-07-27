#!/bin/bash
# sample file for running snaptool as a docker container
# the wekasolutions/snaptool docker image can be downloaded from docker hub
#
config_dir=$PWD
auth_dir=$HOME/.weka
config_fname=$config_dir/snaptool.yml
time_zone=US/Eastern

if [[ ! -f $config_fname ]]; then echo "Config file '$config_fname' missing.  Exiting."; exit 1; fi

# some OS variants may not have this syslog option; if it doesn't exist, don't set it up
if [[ -e /dev/log ]]; then syslog_mount='--mount type=bind,source=/dev/log,target=/dev/log'; fi

docker run -d --network='host' --restart always \
    -e TZ=$time_zone \
    $syslog_mount \
    --mount type=bind,source=$config_dir,target=/weka \
    --mount type=bind,source=$auth_dir,target=/weka/.weka \
    --mount type=bind,source=/etc/hosts,target=/etc/hosts \
    --name weka_snaptool \
    wekasolutions/snaptool -vv
