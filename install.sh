#!/bin/bash
# install script for running snaptool as a service, using the snaptool.service systemd unit file

if  [[ $1 == '' ]]; then
  destdir=/opt/weka/snaptool
else
  destdir=$1
fi
scriptdir=$(dirname $(readlink -f "$0"))

ymlfound="no"
if [[ $scriptdir != $destdir ]]; then
  echo "Installing to $destdir..."
  if [[ ! -e $destdir ]]; then
    echo "   Creating $destdir"
    mkdir -p $destdir
  else
    if [[ -f $destdir/snaptool.yml ]]; then
      echo "   Backing up existing snaptool.yml"
      ymlfound="yes"
      cp $destdir/snaptool.yml $destdir/snaptool.yml.sav
    fi
  fi
  cp $scriptdir/snaptool $destdir
  cp $scriptdir/snaptool.yml $destdir
  cp $scriptdir/snaptool-example.yml $destdir
  if [[ $ymlfound == "yes" ]]; then
    echo "   Restoring existing snaptool.yml"
    cp $destdir/snaptool.yml.sav $destdir/snaptool.yml
  fi
fi

echo "   Testing cluster connection..."
$destdir/snaptool --test-connection-only -c $destdir/snaptool.yml
if [[ $? == 1 ]]; then
  echo "Connection test failed."
  echo "Please check for errors in the snaptool.yml file or network connectivity problems, " \
       "then try running install.sh again"
  exit
fi

if [[ $destdir != '/opt/weka/snaptool' ]]; then
  echo "Install location is not /opt/weka/snaptool - updating snaptool.service file to point to current directory"
  sedstr=$(echo s%/opt/weka/snaptool%$destdir%g)
  cp $scriptdir/snaptool.service $scriptdir/snaptool.service.orig
  sed -i $sedstr $scriptdir/snaptool.service
fi
cp $scriptdir/snaptool.service /etc/systemd/system

systemctl enable /etc/systemd/system/snaptool.service
systemctl start snaptool.service
sleep 1
echo "service installed and started... snaptool.service status:"
sleep 1
systemctl status snaptool.service

