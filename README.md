# snaptool
Weka Cluster Snaphot Manager

A solution that implements snapshot management for Weka Clusters

# Features

- Schedule snapshots monthly, daily, or at multiple (minute granularity) intervals during a daily schedule.
- Retention rules - each schedule controls the number of snapshot copies to retain.
- Expired snapshots are automatically deleted as the schedule exceeds the specified retention.  
- Optionally, snapshots can automatically be uploaded to an S3 Object Store, for tiering enabled file systems.  Snapshots in object stores are also deleted based on the retention rule for a schedule.
- Optionally, snapshots can be uploaded to a remote S3 Object Store, as a back up.  When snapshots are deleted locally, these remote snapshot copies are not deleted.  They are available for restore via the locator ID beyond the life of the original snapshot.
- Snapshots are created per schedules.   Uploads to object stores and deletes occur in a background process via a background queue.

- Note: Configuration files from releases before 1.0.0 are not compatible with 1.0.0 and above.   They will need to be modified to use the new syntax.

# Installation

The latest binary release of snaptool can be downloaded as a tarball from https://github.com/weka/snaptool/releases/latest.  Download it to a temporary location or to /opt/weka.

Before proceeding, make a copy of your existing snaptool.yml, if a previous version exists.   

Next, extract the snaptool bundle:

    tar xvf snaptool-<version>.tar

The extracted configuration file snaptool/snaptool.yml file should now need to be edited for the local environment (or replaced if you have a previous, compatible, version).  See details below for configuration syntax.  


Tips:
- snaptool will not start without the ability to connect to a cluster, so you must edit the yml file to include valid cluster hosts
- If an older version of snaptool exists, please stop all related processes before installing, and backup your yml file
- Run the installer with administrator/root privileges
- The snaptool.service file can be edited if other snaptool command line options need to be provided
- The installer will try to preserve an existing snaptool.yml file, if it exists in the install destination directory

Once the snaptool.yml file contains connection, filesystem, and schedule information for the local weka cluster, snaptool can be installed as a systemd service as follows:
  
    cd snaptool
    ./install.sh

The installer does the following:
- Modifies the systemd unit file (snaptool.service) to the installation target directory if it isn't /opt/weka/snaptool.
- Copies the executable and yml file to the installation directory (typically /opt/weka/snaptool) - if a configuration yml file exists, the installer attempts to preserve it
- Briefly tests connectivity to the weka cluster, to validate cluster settings in the snaptool.yml file
- Copies the systemd unit file (snaptool.service) to /etc/systemd/system
- Enables and starts the snaptool.service service via systemctl

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

            upload: defaults to no/False - 'Local' or 'True' uploads a copy of the snapshot to the local object store associated with the filesystem (the tiering object store).  'Remote' will upload a copy of the snapshot to the object store designated as 'Remote' for the filesystem.



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

The latest release of snaptool can be downloaded from docker hub ('docker pull wekasolutions/snaptool').   A sample docker-run.sh file that provides the necessary parameters to run the snaptool docker image is included in the binary release mentioned above; its contents are shown here also.  A sample snaptool.yml is also included.

The configuration yml file is expected to be in the current directory when running within docker.

Logs will be created in a 'logs' directory in the current directory.



```
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

```

