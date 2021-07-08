# snaptool
Weka Cluster Snaphot Manager


A solution that implements snapshot management for Weka Clusters

# Use Overview:  

snaptool consists of a daemon that manages snapshots

# Features

Schedule snapshots to be taken for each filesystem, with the ability to set a specific number of each type of snapshot to keep.   Expired snapshots are automatically deleted.  Optionally upload snapshots to Object Store automatically.

A default snapshot schedule group is automatically defined with the following parameters:

    Monthly, 1st of the month at midnight, retain 6 snaps
    
    Weekly, Sunday at 00:00 (midnight Sat), retain 8 snaps
    
    Daily, Monday-Saturday at 00:00 (midnight), retain 14 snaps
    
    Hourly, Monday-Friday, 9am-5pm taken at top of the hour, retain 10 snaps

# Configuration

The user may define a custom Schedule in a YAML configuration file - default file is 'snaptool.yml' in the directory snaptool runs.

Filesystems are listed in the YAML file, and define which Schedule they will use there.  Each filesystem line looks like <fsname>: <schedulegroup1 or schedule1>,<schedulegroup2 or schedule2>...  schedules that are within a scheculegroup cannot be assigned separately from the group.  The groupname must be used.

To indicate that a particular subschedule (ie: monthly, weekly) should not run, set the "retain" to 0.  A retain of 0 will cause all snapshots for that (sub)schedule to be deleted.

Using the example configuration file (YAML file), define your filesystems and which schedule(s) they should use.  Also define custom schedules in the YAML file.  Keywords should be self-documenting.

It it suggested to run the utility via systemd with auto restart set.

# Schedule Syntax
           
Each shedule has the following syntax:                       

    _schedulegroupname_:  (optional)

        _schedulename_:

            every: (required) 'month' | 'day' | list of months | list of days

                'day' or list of days 
                    - takes a snap at time specified by at: on the specified day(s)
                    - 'day' is equivalent to specifying all 7 days of the week
                    - (see also interval: <number of minutes> and until:)
                    - list of days can be 3 character day abbreviation, or full day names.  For example:
                        Mon,Tue
                        Monday,Tuesday,Wednesday,Thursday,Friday
                'month' or list of months 
                    - takes a snap on <day:> (integer 1..31) of the month, at time specified by <at:>  
                    - 'month' is equivalent to specifying all 12 months
                    - day: defaults to 1, first day of the month
                    - if day > last day of a month (example: day is 31 and the month is April), 
                        then the snap is taken on the last day of the month
                    - list of months can be 3 character mon abbreviations, or full month names.  For eample:
                        "Jan,Jul"
                        "January,April,Aug,Oct"
            at: time (defaults to midnight - 0000)
                format accepts times like "9am", "9:15am" "2300" etc  examples:
                    at: 9am
                    at: 0900
                    at: 9:05pm

            interval: 
                only applicable for schedules by day, not month ('day' or list of days)
                if interval: is not provided, a single snapshot per day is taken at "at:"
                if interval: is provided - at: and until: provide the start and end times for the snaps taken
                first snap is taken at 'at:' then every <interval:> minutes thereafter until 'until:' is reached
                        Interval will only attempt a snap within a day.  They do not roll over to a 2nd day or beyond.
                        So at: + interval: should always yield a time less than until:.  Otherwise only one snap is taken.
            at: defaults to 0000 
            until: default to 2359
            retain: 0 disables schedule, otherwise this is the number of snapshots kept.  Defaults to 1
            upload: defaults to no/false - yes/true uploads the snapshot to the object store associated with the filesystem


example snaptool.yml:

    filesystems:
        fs01: default
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



# Snapshot Naming

The format of the snapshot names is Schedule.YYYY-MM-DD_HHMM, with the access point @GMT-YYYY.MM.DD-HH.MM.SS.   For example, a snapshot might be named hourly.2021-03-10_1700 and have the access point @GMT-2021.03.10-17.00.00.  The snapshot name will be in the local timezone, and the access point in GMT.  (in this example, the server timezone is set to GMT time)
    
For grouped snapshots, the name will be schedulegroupname_schedulename if a schedulegroupname exists.   The full name can't be longer than 18 characters.

When deleting snapshots, they are sorted and the oldest deleted until there are "retain" snapshots left for the particular Schedule.

Note that we are unable to distinguish between user-created and snapshot manager-created snapshots, other than by the name, so when creating user-created snapshots, you should use a different naming format; if the same naming format is used, the user-created snapshots may be selected for deletion automatically.

# Command-line Arguments

The snaptool takes a "cluster spec" as a required argument.  This is a comma-separated list of weka hosts (ip addrs or names) with an optional :authfile.   The auth file comes from the "weka user login" command, is generally in the ~/.weka directory, and contains authorization tokens so that the snaptool program can communicate with the weka cluster.

An example "cluster spec" would be:

    weka1,weka2,weka3:~/.weka/auth-file.json

An optional verbosity can also be specified with the -v parameter.   Adding more than one "v" increases verbosity level (ie: "-vvv").

# Running in Docker

```
docker run -d -v /dev/log:/dev/log \
    --mount type=bind,source=$PWD/snaptool.yml,target=/weka/snaptool.yml \
    wekasolutions/snaptool -vvv ip-172-31-13-179,ip-172-31-12-28,ip-172-31-1-140
    
docker run -d --network=host \
    --mount type=bind,source=/root/.weka/,target=/weka/.weka/ \
    --mount type=bind,source=/dev/log,target=/dev/log \
    --mount type=bind,source=/etc/hosts,target=/etc/hosts \
    --mount type=bind,source=$PWD/snaptool.yml,target=/weka/snaptool.yml \
    wekasolutions/snaptool -vvv ip-172-31-13-179,ip-172-31-12-28,ip-172-31-1-140    
```

