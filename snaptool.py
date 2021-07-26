
# Weka Snapshot Management Daemon
# Vince Fleming
# vince@weka.io
#
# updated for new scheduling - Bruce Clagett

from operator import attrgetter, itemgetter
import os
import sys
import argparse
import platform
import time

import wekalib.exceptions
import yaml
import urllib3
import logging.handlers

import datetime
from datetime import timezone

import wekalib.signals as signals
import wekalib.wekacluster as wekacluster
import snapshots
import background

VERSION = "1.0.0 b"

# get the root logger, get snaptool logger
log = logging.getLogger()
if os.environ.get('INITIAL_LOG_LEVEL') is not None:
    log.setLevel(os.environ.get('INITIAL_LOG_LEVEL'))
else:
    log.setLevel(logging.WARNING)  # to start
snaplog = logging.getLogger("snapshot_f")
action_history_log_file = "snaptool.log"


def now():
    return datetime.datetime.now()

def setup_logging_initial():
    background.create_log_dir_file(action_history_log_file)

    snaptool_f_handler = logging.handlers.RotatingFileHandler(action_history_log_file,
                                                              maxBytes=10 * 1024 * 1024, backupCount=2)
    snaptool_f_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    snaplog.addHandler(snaptool_f_handler)
    snaplog.setLevel(logging.INFO)
    # snaplog file is intended for high level action logging (create/delete snapshots, etc, distinct
    # from other logging), so don't propagate to root logger
    snaplog.propagate = False

    syslog_format = "%(process)s:%(filename)s:%(lineno)s:%(funcName)s():%(levelname)s:%(message)s"
    console_format = "%(asctime)s:%(levelname)7s:%(filename)s:%(lineno)s:%(funcName)s():%(message)s"

    # create handler to log to stderr
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(console_format))

    log.addHandler(console_handler)
    log.info("--------------------------Program (re)start initial----------------------------")
    # create handler to log to syslog
    log.info(f"setting syslog on {platform.platform()}")
    if platform.platform()[:5] == "macOS":
        syslog_addr = "/var/run/syslog"
    else:
        syslog_addr = "/dev/log"
    if os.path.exists(syslog_addr):
        syslog_handler = logging.handlers.SysLogHandler(syslog_addr)
        syslog_handler.setFormatter(logging.Formatter(syslog_format))
        # add handlers to root logger
        if syslog_handler is not None:
            log.addHandler(syslog_handler)
    else:
        log.info(f"{syslog_addr} not found - no syslog handler set")

def set_logging_levels(snaptool_level, snapshots_level=logging.ERROR,
                       background_level=logging.ERROR, wekalib_level=logging.ERROR):
    log.setLevel(snaptool_level)
    log.info("-------------------------Setting new log levels-------------------------------")

    urllib3.add_stderr_logger(level=logging.ERROR)

    logging.getLogger("wekalib.wekacluster").setLevel(wekalib_level)
    logging.getLogger("wekalib.wekaapi").setLevel(wekalib_level)
    logging.getLogger("wekalib.sthreads").setLevel(wekalib_level)
    logging.getLogger("wekalib.circular").setLevel(wekalib_level)
    logging.getLogger("background").setLevel(background_level)
    logging.getLogger("snapshots").setLevel(snapshots_level)

class ClusterConnection(object):
    def __init__(self, clusterspec, authfile, force_https, cert_check):
        self.weka_cluster = None
        self.clusterspec = clusterspec
        self.authfile = authfile
        self.force_https = force_https
        self.verify_cert = cert_check
        self.connect_datetime = datetime.datetime.min

    def connect(self):
        connected = False
        try:
            self.weka_cluster = wekacluster.WekaCluster(self.clusterspec, self.authfile,
                                                        force_https=self.force_https, verify_cert=self.verify_cert)
            self.connect_datetime = now()
            connected = self.weka_cluster
        except Exception as exc:
            log.error(f"Failed to connect to cluster hosts: {self.clusterspec} with authfile: {self.authfile}.  {exc}")
        return connected

    def call_weka_api(self, method, parms):
        raise_exc = None
        max_retries = 20
        sleep_wait = 5
        for i in range(max_retries):
            try:
                result = self.weka_cluster.call_api(method=method, parms=parms)
                return result
            except wekalib.exceptions.APIError as exc:
                raise_exc = exc
                errmsg = f"{exc}"
                err_type = type(exc).__name__
                log.warning(f"wekalib exception for ('{method}', {parms}):")
                log.warning(f"        wekalib message: '{err_type}: {exc}'")
                if method == "snapshot_create":
                    if errmsg.find("already exists") > 0:   # 'name already exists' or 'accessPoint already exists'
                        return None
                log.warning(f"Will try again after {sleep_wait} seconds (retry {i + 1} of {max_retries})...")
                time.sleep(sleep_wait)
                if i >= 2:  # first couple times just wait and try again.
                    # After that, reconnect to cluster each failure, increase wait time
                    # could re-read config here in case filesystem name or authfile changed
                    # or other config fixed/changed?
                    # but it will get re-read after 5 min anyway, so no?
                    log.warning(f"Reconnecting to cluster before next retry")
                    self.connect()
                    sleep_wait = 20
                i += 1
                continue
        raise raise_exc

    def check_cluster_connection(self):
        result = self.call_weka_api('status', {})
        log.debug(f"Cluster connected: {self.weka_cluster} io_status: {result['io_status']}")
        return True


class ScheduleGroup(object):
    def __str__(self):
        return f"(Group {self.name}: {len(self.entries)} entries; filesystems: {self.filesystems})"

    def __init__(self, name):
        self.name = name
        self.entries = []
        self.filesystems = []
        self.sort_priority = 9999
        self.no_upload = True
        self.next_snap_time = datetime.datetime.max

    def log_pp(self, logger_object, level):
        msg = f"Group {self.name} (next snap: {self.next_snap_time})"
        msg += f" for filesystems {self.filesystems}, upload: {not self.no_upload}"
        logger_object.log(level, msg)
        for e in self.entries:
            logger_object.log(level, f"   {e.name}:\t{e.nextsnap_dt}\t({str(e)})")


def config_syntax_error(args, message):
    logging.error(f"Error in file {args.configfile}: {message} - please fix")
    time.sleep(10)

def syntax_check_top_level(args, config):
    msg = ''
    if 'cluster' in config:
        msg += "'cluster' found "
        c_found = True
    else:
        msg += "'cluster' not found. "
        c_found = False
    if 'filesystems' in config:
        msg += f"'filesystems' found. "
        fs_found = True
    else:
        msg += f"'filesystems' not found. "
        fs_found = False
    if 'schedules' in config:
        msg += f"'schedules' found. "
        s_found = True
    else:
        msg += f"'schedules' not found. "
        s_found = False
    log.info(f"Config parse: {msg}")
    if s_found and fs_found and c_found:
        log.info(f"Config top level check ok. {msg}")
    else:
        config_syntax_error(args, msg)

def parse_bool(bool_str):
    if str(bool_str).lower() in ["yes", "true", "1"]:
        return True
    elif str(bool_str).lower() in ["no", "false", "0"]:
        return False
    else:
        log.error(f"Invalid boolean spec; should be 'yes', 'no', 'true' or 'false': {bool_str} in config file")
        log.error(f"Assuming False")
        return False

def create_cluster_connection(config):
    # returns a cluster connection object
    cluster_yaml = {}
    if 'cluster' in config:
        cluster_yaml = config['cluster']
    if 'hosts' in cluster_yaml:
        clusterspec = cluster_yaml['hosts']
    else:
        log.error(f"A clusterspec is required in the config file.  Exiting")
        clusterspec = ''
    if 'auth_token_file' in cluster_yaml:
        authfile = cluster_yaml['auth_token_file']
    else:
        log.warning(f"No auth file specified, trying auth-token.json")
        authfile = "auth-token.json"
    if 'force_https' in cluster_yaml:
        force_https = parse_bool(cluster_yaml['force_https'])
    else:
        force_https = False
    if 'verify_cert' in cluster_yaml:
        verify_cert = parse_bool(cluster_yaml['verify_cert'])
    else:
        verify_cert = True
    result = ClusterConnection(clusterspec, authfile, force_https, verify_cert)
    return result

def config_parse_fs_schedules(args, config):
    resultsdict = {}
    syntax_check_top_level(args, config)

    filesystems = config['filesystems']
    schedules = config['schedules']
    log.debug(f"JSON filesystems: {filesystems}")
    log.debug(f"JSON schedules: {schedules}")
    for schedname, schedule_spec in schedules.items():
        new_group = ScheduleGroup(schedname)
        resultsdict[schedname] = new_group
        if "every" in schedule_spec.keys():    # single schedule item without a sub-schedule name
            entry = snapshots.parse_schedule_entry(None, schedname, schedule_spec)
            new_group.entries.append(entry)
        else:
            for schedentryname, schedentryspec in schedule_spec.items():
                entry = snapshots.parse_schedule_entry(schedname, schedentryname, schedentryspec)
                new_group.entries.append(entry)
    for fs_name, fs_schedulegroups in filesystems.items():
        if isinstance(fs_schedulegroups, str):
            fs_schedulegroups = snapshots.comma_string_to_list(fs_schedulegroups)
            filesystems[fs_name] = fs_schedulegroups
        log.info(f"{fs_name}, {fs_schedulegroups}")
        for sched_name in fs_schedulegroups:
            if sched_name not in resultsdict.keys():
                config_syntax_error(args, f"Schedule {sched_name}, listed for filesystem {fs_name}, not found")
            else:
                resultsdict[sched_name].filesystems.append(fs_name)
    return resultsdict

def parse_snaptool_args():
    argparser = argparse.ArgumentParser(description="Weka Snapshot Management Daemon")
    argparser.add_argument("-c", "--configfile", dest='configfile', default="./snaptool.yml",
                           help="specify a file other than './snaptool.yml' for the config file")
    argparser.add_argument("-v", "--verbosity", action="count", default=0,
                           help="increase output verbosity; -v, -vv, -vvv, or -vvvv")
    argparser.add_argument("--version", dest="version", default=False, action="store_true",
                           help="Display version number")
    # hidden argument for connection test only.   exits program with 1 if connection fails, 0 otherwise.
    # Also prints "Connection Succeeded" or "Connection Failed"
    argparser.add_argument("--test-connection-only", dest="test_connection_only",
                           action='store_true', default=False, help=argparse.SUPPRESS)
    args = argparser.parse_args()

    if args.version:
        log.info(f"{sys.argv[0]} version {VERSION}")
        print(f"{sys.argv[0]} version {VERSION}")
        sys.exit(0)

    if args.verbosity == 0:
        loglevel = logging.ERROR
    elif args.verbosity == 1:
        loglevel = logging.WARNING
    elif args.verbosity == 2:
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG

    return args, loglevel

def update_snaptimes_sort_and_clean(snapgrouplist, now_dt):
    unused_list = []
    log.debug(f"schedule groups before unused check: {len(snapgrouplist)}")
    for sg in snapgrouplist:
        if len(sg.filesystems) == 0:
            unused_list.append(sg)
    log.warning(f"Unused schedules: {[s.name for s in unused_list]}")
    for sg in unused_list:
        snapgrouplist.remove(sg)
    log.debug(f"schedule groups after unused check:"
              f" {len(snapgrouplist)} {[(s.name, s.filesystems) for s in snapgrouplist]}")
    # update snaptimes in entries and in SnapGroups, and sort
    for sg in snapgrouplist:
        for entry in sg.entries:
            entry.calc_next_snaptime(now_dt)
        sg.entries.sort(key=attrgetter('nextsnap_dt', 'sort_priority', 'no_upload'))
        log.debug(f"Sorted entries for {sg.name} fs: {sg.filesystems} entries: {[e.sort_priority for e in sg.entries]}")
        if len(sg.entries) > 0:
            sg.next_snap_time = sg.entries[0].nextsnap_dt
            sg.sort_priority = sg.entries[0].sort_priority
            sg.no_upload = sg.entries[0].no_upload
    snapgrouplist.sort(key=attrgetter('next_snap_time', 'sort_priority', 'no_upload'))
    log.info(f"schedule groups after sort:"
             f" {len(snapgrouplist)} {[(s.name, str(s.next_snap_time), s.filesystems) for s in snapgrouplist]}")

def log_snapgrouplist(snapgroup_list):
    for sg in snapgroup_list:
        sg.log_pp(log, logging.DEBUG)

def get_snapgroups_for_snaptime(snapgroup_list, snaptime):
    result = []
    for item in snapgroup_list:
        if item.next_snap_time == snaptime:
            result.append(item)
        else:
            break
    return result

def get_snaps_dict_by_fs(snapgroups_for_nextsnap, next_snap_time):
    results = {}
    for sg in snapgroups_for_nextsnap:
        log.debug(f"  snapgroup '{sg.name}' filesystems: {sg.filesystems}")
        for fs in sg.filesystems:
            if fs not in results:
                log.debug(f"        {sg.name} {sg.entries[0].name} will snap {fs} at {next_snap_time} ")
                results[fs] = sg.entries[0]
            else:
                log.debug(f"        conflicting snap of {fs} ignored")
    log.debug(f"filesystems: {list(results.keys())} - {len(results)} entries")
    return results

def next_snaps(parsed_schedules_dict):
    sg_list = list(parsed_schedules_dict.values())
    update_snaptimes_sort_and_clean(sg_list, now())
    log_snapgrouplist(sg_list)
    next_snap_time = sg_list[0].entries[0].nextsnap_dt    # because it's sorted this works
    snapgroups_for_nextsnap = get_snapgroups_for_snaptime(sg_list, next_snap_time)
    log.debug(f"next snap time: {next_snap_time}, {len(snapgroups_for_nextsnap)} snaps")
    return next_snap_time, get_snaps_dict_by_fs(snapgroups_for_nextsnap, next_snap_time)

def get_snapshots(cluster):
    snapshot_list = cluster.call_weka_api("snapshots_list", {})
    return snapshot_list

def create_snapshot(cluster, fs, name, access_point_name, upload):
    try:
        status = cluster.call_weka_api(method="snapshots_list", parms={'file_system': fs, 'name': name})
        if len(status) == 1:
            snaplog.warning(f"Snapshot fs/name {fs}/{name} already exists; skipping")
            return
        created_snap = cluster.call_weka_api(method="snapshot_create", parms={
                "file_system": fs,
                "name": name,
                "access_point": access_point_name,
                "is_writable": False})
        if created_snap is None:
            log.info(f"   snap {fs}/{name} already exists")
        else:
            snaplog.info(f"   snap {fs}/{name} created")
        if upload:
            background.QueueOperation(cluster.weka_cluster, fs, name, "upload")
    except Exception as exc:
        log.error(f"Error creating snapshot {name} on filesystem {fs}: {exc}")

def create_new_snaps(cluster, next_snaps_dict, next_snap_time):
    for fs, snap in next_snaps_dict.items():
        access_point_name = next_snap_time.astimezone(timezone.utc).strftime(
            "@GMT-%Y.%m.%d-%H.%M.%S")  # windows format
        # don't need century, or date at all really, because snap creation time is used for delete, comparisons
        # date in the name is really for convenience in displays
        next_snap_name = snap.name + "." + next_snap_time.strftime("%y%m%d%H%M")
        log.info(f"Creating fs/snap {fs}/{next_snap_name} (name len={len(next_snap_name)})")
        create_snapshot(cluster, fs, next_snap_name, access_point_name, snap.upload)


def get_fs_snaps(all_snaps, fs, schedname):
    # return snaps for fs that are named <schedname>.<something>, return them sorted by creation time
    log.debug(f"Getting snaps specific to {fs} and {schedname}")
    snaps_for_fs = []
    for s in all_snaps:
        snap_name = s['name'].split('.')
        if s['filesystem'] == fs and len(snap_name) == 2 and snap_name[0] == schedname:
            snaps_for_fs.append(s)
    snaps_for_fs.sort(key=itemgetter('creationTime'))
    return snaps_for_fs

def delete_old_snaps(cluster, parsed_schedules_dict):
    # look at all defined schedule groups, not just last loop snaps
    # in case retentions have changed (for example, to 0)
    all_snaps = get_snapshots(cluster)
    sg_list = list(parsed_schedules_dict.values())
    for sg in sg_list:
        for entry in sg.entries:
            for fs in sg.filesystems:
                # snaps = [s for s in all_snaps if s['name'].split(".")[0] == entry.name and s['filesystem'] == fs]
                snaps = get_fs_snaps(all_snaps, fs, entry.name)
                # snaps.sort(key=itemgetter('creationTime'))
                if len(snaps) > entry.retain:
                    num_to_delete = len(snaps) - entry.retain
                    snaps_to_delete = snaps[:num_to_delete]
                    for s in snaps_to_delete:
                        log.info(f"Queueing fs/snap: {fs}/{s['name']} for delete")
                        background.QueueOperation(cluster.weka_cluster, fs, s['name'], "delete")


def get_configfile_config(filename):
    with open(filename, 'r') as f:
        config = yaml.load(stream=f, Loader=yaml.BaseLoader)
    log.debug(config)
    return config

def connection_info_changed(existing_connection, new_connection):
    if (existing_connection.authfile != new_connection.authfile or
            existing_connection.clusterspec != new_connection.clusterspec or
            existing_connection.force_https != new_connection.force_https or
            existing_connection.verify_cert != new_connection.verify_cert):
        return True
    else:
        return False

def main():
    setup_logging_initial()
    # handle signals (ie: ^C and such)
    signals.signal_handling()
    args, loglevel = parse_snaptool_args()
    set_logging_levels(loglevel, snapshots_level=loglevel, background_level=logging.INFO)

    # tests
    run_tests = True
    if run_tests:     # scheduling computation tests
        snapshots.run_schedule_tests()

    connected = False
    cluster_connection, schedules_dict = None, None
    while not connected:
        config = get_configfile_config(args.configfile)
        schedules_dict = config_parse_fs_schedules(args, config)
        log.info("Attempting cluster connection...")
        cluster_connection = create_cluster_connection(config)
        connected = cluster_connection.connect()
        if args.test_connection_only:
            if connected:
                print("Connection Succeeded")
                sys.exit(0)
            else:
                print("Connection Failed")
                sys.exit(1)
        if not connected:
            log.error(f"Connection to {cluster_connection.clusterspec} failed.  "
                      f"Sleeping then reloading config, and trying again.")
            time.sleep(60)

    log.warning("Replaying background operation intent log...")
    background.intent_log.replay(cluster_connection.weka_cluster)

    # do this once at beginning so we don't wait for next snap time to clean up anything missed from previous runs
    # depending on timing, may end up with "already deleted" messages in logs
    delete_old_snaps(cluster_connection, schedules_dict)

    reload_interval = 300
    last_reload_time = time.time()

    while True:
        if (time.time() - last_reload_time) > reload_interval:
            log.info(f"--------------- Reloading configuration file {args.configfile}")
            config = get_configfile_config(args.configfile)
            schedules_dict = config_parse_fs_schedules(args, config)
            new_connection = create_cluster_connection(config)
            if connection_info_changed(cluster_connection, new_connection):
                log.info(f"-------------------- Reconnecting with different cluster configuration...")
                new_connection.connect()
                cluster_connection = new_connection
            last_reload_time = time.time()

        next_snap_time, next_snaps_dict = next_snaps(schedules_dict)
        sleep_time = round((next_snap_time - now()).total_seconds(), 1)
        fs_msg_str = f"{ {fs:s.name for fs, s in next_snaps_dict.items()} }"
        if sleep_time <= 0:
            sleep_msg = f"Snap now: ({next_snap_time}): {fs_msg_str}"
            sleep_time = 0
        else:
            sleep_msg = f"Sleep until {next_snap_time} ({sleep_time}s), then snap: {fs_msg_str}"
        log.info(sleep_msg)
        time.sleep(sleep_time)

        create_new_snaps(cluster_connection, next_snaps_dict, next_snap_time)
        delete_old_snaps(cluster_connection, schedules_dict)

        # prevent same snap attempts from running again in the same minute
        sleep_time = round(60 - (now() - next_snap_time).total_seconds(), 1)
        if sleep_time > 0:
            log.info(f"Sleeping for {sleep_time} seconds before next loop")
            time.sleep(sleep_time)


if __name__ == '__main__':
    main()
