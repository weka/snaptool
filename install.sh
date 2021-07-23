#!/bin/bash
# install script for running snaptool as a service, using the snaptool.service systemd unit file

scriptdir=$(dirname $(readlink -f "$0"))
if [[ $scriptdir != '/opt/weka/snaptool' ]]; then
  echo "Install location is not /opt/weka - changing snaptool.service file to point to current directory"
  sedstr=$(echo s%/opt/weka/snaptool%$scriptdir%g)
  sed -i $sedstr $scriptdir/snaptool.service
fi

./snaptool --test-connection-only
if [[ $? == 1 ]]; then
  echo "Connection test failed."
  echo "Please check for errors in the snaptool.yml file or network connectivity problems, " \
       "then try running install.sh again"
  exit
fi

cp $scriptdir/snaptool.service /etc/systemd/system

systemctl enable /etc/systemd/system/snaptool.service
systemctl start snaptool.service
echo "service installed and started... startup output:"
journalctl -u snaptool.service

