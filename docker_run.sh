#!/bin/bash
# sample file for running snaptool as a docker container
# the wekasolutions/snaptool docker image can be downloaded from docker hub
#
# the config_file is expected to be in the current directory when running within docker.
# logs will be created in a 'logs' directory in the current directory.
#
config_file=snaptool.yml
time_zone=US/Eastern
auth_dir=$HOME/.weka

mkdir -p logs ; chown 472 logs

if [[ ! -f $config_file ]]; then echo "Config file '$config_file' missing.  Exiting."; exit 1; fi

# some OS variants may not have this syslog option; if it doesn't exist, don't set it up
if [[ -e /dev/log ]]; then syslog_mount='--mount type=bind,source=/dev/log,target=/dev/log'; fi

docker run --network='host' --restart always -e TZ=$time_zone -d \
    $syslog_mount \
    --mount type=bind,source=$PWD,target=/weka \
    --mount type=bind,source=$auth_dir,target=/weka/.weka,readonly \
    --mount type=bind,source=/etc/hosts,target=/etc/hosts,readonly \
    --name weka_snaptool \
    wekasolutions/snaptool -vv -c $config_file

