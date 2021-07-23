# snaptool
Weka Cluster Snaphot Manager

A solution that implements snapshot management for Weka Clusters

# Features

Schedule snapshots to be taken for each filesystem listed in snaptool.yml, with the ability to set a specific number of each specified snapshot to keep.   Expired snapshots are automatically deleted.  Optionally, snapshots can automatically be uploaded to an S3 Object Store, if the filesystem has tiering enabled.

# Installation

The latest binary release of snaptool can be downloaded as a tarball from https://github.com/weka/snaptool/releases; it is recommended this be downloaded to /opt/weka/snaptool.

Once downloaded, untar the file

    tar xvf snaptool-<version>.tar

The extracted configuration file snaptool/snaptool.yml file will now need to be edited for the local environment.  See details below for configuration syntax.  

Once the snaptool.yml file contains connection, filesystem, and schedule information for the local weka cluster, snaptool can be installed as a service as follows.   Note that if snaptool isn't in /opt/weka/snaptool, the snaptool.service file will need to be edited for the correct location first.  snaptool.service can also be edited if other command line options need to be changed.

    cd snaptool
    cp snaptool.service /etc/systemd/services
    systemctl enable /etc/systemd/service/snaptool.service
    systemctl start snaptool.service

Snaptool can also be run in docker - if that is the desired deployment, see the "Running in Docker" section below.

# Configuration

A YAML file provides configuration information. The default configuration file name is snaptool.yml, and a sample snaptool.yml is included.  There are three top-level sections, all of which are required:

    cluster:  
    filesystems: 
    schedules:

Cluster information is in the 'cluster:' section.  The hosts list is required.   Other entries in this section are optional but are recommended for clarity.  See the example snaptool.yml, below, for valid syntax.  Entries allowed are:

    cluster:
        auth_token_file: 
        hosts: 
        force_https: 
        verify_cert: 

Filesystems are in the 'filesystems' section, and these entries define which snapshot schedule(s) will run for the listed filesystems.  Each filesystem line looks like:

    <fsname>:  <schedule1>,<schedule2>...

Schedules Syntax is below.   Schedules that are within a schedule group cannot be assigned separately from the group.  The groupname must be used.

Using the example configuration file (YAML file), define your filesystems and which schedule(s) they should use.  Also define custom schedules in the YAML file.  Schedule keywords and syntax are shown below.

To indicate that a particular schedule (i.e.: monthly, weekly) should not run on a filesystem, set the "retain" to 0, or remove it from the filesystem's schedule list.  

snaptool reloads the YAML configuration file before calculating the next set of snapshot runs, if at least 5 minutes have passed since the last reload.

# Schedule Syntax
           
Each schedule has the following syntax:                       

    <optional schedulegroupname>:  

        <schedulename>:

            every: (required) 'month' | 'day' | list of months | list of days
                'day' or list of days 
                    - takes a snap at time specified by at: on the specified day(s)
                    - 'day' is equivalent to specifying all 7 days of the week
                    - list of days can be 3 character day abbreviation, or full day names.  For example:
                        Mon,Tue
                        Monday,Tuesday,Wednesday,Thursday,Friday
                    - see also 'interval:' <number of minutes> and 'until:'
                'month' or list of months 
                    - takes a snap on <day:> (integer 1..31) of the month, at time specified by <at:>  
                    - 'month' is equivalent to specifying all 12 months
                    - day: defaults to 1, first day of the month
                    - if day > last day of a month (example: day is 31 and the month is April), 
                        then the snap is taken on the last day of the month
                    - list of months can be 3 character mon abbreviations, or full month names.  For eample:
                        "Jan,Jul"
                        "January,April,Aug,Oct"

            at: time - defaults to '0000' (midnight)
                - format accepts times like "9am", "9:15am" "2300" etc.  Some valid examples:
                    at: 9am
                    at: 0900
                    at: 9:05pm

            interval: <number of minutes>
                - number of minutes between snapshots
                - only applicable for schedules by day, not month ('day' or list of days)
                - if 'interval:' is not provided, a single snapshot per day is taken at "at:"
                - if 'interval:' is provided - 'at:' and 'until:' provide the start and end times for the snaps taken
                - first snap is taken at 'at:' time, then every <interval:> minutes thereafter until 'until:' is reached
                        Interval will only attempt snaps within a day, between times specified by 'at:' and 'until:'.  
                        So this value, added to 'at:' time, should always yield a time less than 'until:', otherwise it is ignored.

            until: defaults to '2359'
                - the latest time that an interval-based snapshot can be created

            retain: defaults to 4.  This is the number of snapshots kept. 0 disables the schedule. 

            upload: defaults to no/False - yes/True uploads the snapshot to the object store associated with the filesystem



example snaptool.yml:

    cluster:
        auth_token_file: auth-token.json
        hosts: vweka1,vweka2,vweka3
        force_https: True   # only 3.10+ clusters support https
        verify_cert: False  # default cert cannot be verified

    filesystems:
        fs01: default
        fs02: Weekdays-6pm, Weekends-noon

    schedules:
        default:
            monthly:
                every: month
                retain: 6
                # day: 1   (this is default)
                # at: 0000 (this is default)
            weekly:
                every: Sunday
                retain: 8
                # at: 0000 (this is default)
            daily:
                every: Mon,Tue,Wed,Thu,Fri,Sat
                retain: 14
                # at: 0000 (this is default)
            hourly:
                every: Mon,Tue,Wed,Thu,Fri
                retain: 10
                interval: 60
                at: 9:00am
                until: 5pm
        Weekdays-6pm:
            every: Mon,Tue,Wed,Thu,Fri
            at: 6pm
            retain: 4
        Weekends-noon:
            every: Sat,Sun
            at: 1200
            retain: 4

# Snapshot Naming

The format of the snapshot names is schedulename.YYMMDDHHMM, with the access point @GMT-YYYY.MM.DD-HH.MM.SS.   For example, a snapshot might be named 'Weekends-noon.2103101200' and have the access point @GMT-2021.03.10-12.00.00.  The snapshot name will be in the local timezone, and the access point in GMT.  (In this example, the server timezone is set to GMT time)
    
For grouped snapshots, the name will be schedulegroupname_schedulename.   The full name can't be longer than 18 characters.  For example, 'default' schedule group with an 'hourly' schedule in it might be named 'default_hourly.YYMMDDHHMM'.

When deleting snapshots automatically, based on the 'retain:' keyword, snapshots for a schedule and filesystem are sorted by creation time, and the oldest snapshots will be deleted until there are "retain:" snapshots left for the applicable Schedule and filesystem.

Note that snaptool does not distinguish between user-created and snaptool-created snapshots, other than by the name, so when creating user-created snapshots, you should use a different naming format; if the same naming format is used, the user-created snapshots may be selected for deletion automatically.

# Command-line Arguments

The snaptool command line takes the following optional arguments:

    -c or --configfile followed by a path/filename can be used to specify a file other than ./snaptool.yml for configuration information.

    -v, -vv, -vvv, or -vvvv specify logging verbosity.  More 'v's produce more verbose logging.

Examples:

    # run with some extra logging output, and use a different config file:
    snaptool -v -c /home/user/my-snaptool-config.yml
    # run with a very high level of output logging
    snaptool -vvvv

# Running in Docker

A sample docker-run.sh file is included.   Contents are shown here as an example.

docker pull can be used to get the latest 

```
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
    wekasolutions/snaptool -vv vweka1,vweka2,vweka3
```

