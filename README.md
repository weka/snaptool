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

The user may define a custom Schedule in the YAML configuration file, weka_snapd.yml

Filesystems are listed in the YAML file, and define which Schedule they will use there.

Currently, snapshot uploads to Cloud is unimplemented.

