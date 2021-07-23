#!/bin/bash
# install script for running snaptool as a service, using the snaptool.service systemd unit file

# should check to make sure install dir is /opt/weka
# should check to see if weka agent is installed - if it is maybe query cluster?
# prompt user to verify snaptool.yml

if [[ $PWD != '/opt/weka' ]]; then
  echo "Unless you have changed the snaptool.service, you should run this installer from /opt/weka"
fi
./snaptool --test-connection-only
if [[ $? == 1 ]]; then
  echo "Connection test failed."
  echo "Please check for errors in the snaptool.yml file or network connectivity problems, " \
       "then try running install.sh again"
  exit
fi

cp ./snaptool.service /etc/systemd/system

snaptool enable /etc/systemd/system/snaptool.service
snaptool start snaptool.service
