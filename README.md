# snap_manager
Weka Cluster Snaphot Manager


A solution that implements snapshot management for Weka Clusters

# Use Overview:  

snap_manager consists of a daemon that manages snapshots

# Features

Schedule snapshots to be taken hourly, daily, weekly, monthly for each filesystem, with the ability to set a specific number of each type of snapshot to keep.   Expired snapshots are automatically deleted.  Optionally upload snapshots to Object Store automatically.

A default snapshot schedule is automatically defined with the following parameters:

    Monthly, 1st of the month at midnight, retain 6 snaps
    
    Weekly, Sunday at 00:00 (midnight Sat), retain 8 snaps
    
    Daily, Monday-Saturday at 00:00 (midnight), retain 14 snaps
    
    Hourly, Monday-Friday, 9am-5pm taken at top of the hour, retain 10 snaps

# Configuration

The user may define a custom Schedule in the YAML configuration file, weka_snapd.yml

Filesystems are listed in the YAML file, and define which Schedule they will use there.

Note on config file - do NOT leave any keywords out.  If you want to define several custom schedules, be sure to copy the entire stanza.  To indicate that a particular subschedule (ie: monthly, weekly) should not run, set the "retain" to 0. 

Using the example configuration file (YAML file), define your filesystems and which schedule they should use.  Also define custom schedules in the YAML file.  Keywords should be self-documenting.

It it suggested to run the utility via systemd with auto restart set.

# Snapshot Naming

The format of the snapshot names is <schedule>.YYYY-MM-DD_HHMM
    
When deleting snapshots, they are sorted and the oldest deleted until there are "retain" snapshots left for the particular Schedule.

Note that we are unable to distinguish between user-created and snapshot manager-created snapshots, other than by the name, so when creating user-created snapshots, you should use a different naming format; if the same naming format is used, the user-created snapshots may be selected for deletion automatically.

# Command-line Arguments

The weka_snapd takes a "cluster spec" as a required argument.  This is a comma-separated list of weka hosts (ip addrs or names) with an optional :authfile.   The auth file comes from the "weka user login" command, is generally in the ~/.weka directory, and contains authorization tokens so that the weka_snapd program can communicate with the weka cluster.

An example "cluster spec" would be:

    weka1,weka2,weka3:~/.weka/auth-file.json

An optional verbosity can also be specified with the -v parameter.   Adding more than one "v" increases verbosity level (ie: "-vvv").



