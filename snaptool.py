
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
import importlib_metadata

import yaml
import urllib3
import logging.handlers

import datetime
from datetime import timezone

import wekalib
import wekalib.exceptions
import wekalib.signals as signals
import wekalib.wekacluster as wekacluster
import snapshots
import background

VERSION = "1.0.0"

# get the root logger, get snaptool logger
log = logging.getLogger()
actions_log = logging.getLogger("snapshot_actions_log")
actions_log_file = "snaptool.log"

running_in_docker = os.getenv('IN_DOCKER_CONTAINER', 'NO')
running_as_service = os.getenv('LAUNCHED_BY_SYSTEMD', 'NO')

def version_string():
    return (f"{sys.argv[0]} version: {VERSION}"
            f" wekalib-version={importlib_metadata.version('wekalib')}"
            f" docker={running_in_docker}"
            f" service={running_as_service}")

def parse_snaptool_args():
    argparser = argparse.ArgumentParser(description="Weka Snapshot Management Daemon")
    argparser.add_argument("-c", "--configfile", dest='configfile', default="snaptool.yml",
                           help="specify a file other than 'snaptool.yml' for the config file")
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
        print(version_string())
        sys.exit(0)

    args.configfile = _find_config_file(args.configfile)

    if args.verbosity == 0:
        loglevel = logging.ERROR
    elif args.verbosity == 1:
        loglevel = logging.WARNING
    elif args.verbosity == 2:
        loglevel = logging.INFO
    else:
        loglevel = logging.DEBUG

    return args, loglevel

def now():
    return datetime.datetime.now()

def setup_actions_log():
    log.info(f"Setting up actions log {actions_log_file}")
    resolved_fname = background.create_log_dir_file(actions_log_file)

    snaptool_f_handler = logging.handlers.RotatingFileHandler(resolved_fname,
                                                              maxBytes=10 * 1024 * 1024, backupCount=2)
    snaptool_f_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    actions_log.addHandler(snaptool_f_handler)
    actions_log.setLevel(logging.INFO)
    # actions_log file is intended for high level action logging (create/delete snapshots, etc, distinct
    # from other logging), so don't propagate to root logger
    actions_log.propagate = False

def setup_logging_initial():
    syslog_format = \
        "%(process)5s: %(levelname)-7s:%(filename)-15ss:%(lineno)4d:%(funcName)s(): %(message)s"
    console_format = \
        "%(asctime)s.%(msecs)03d: %(levelname)-7s:%(filename)-15s %(lineno)4d:%(funcName)s(): %(message)s"
    console_date_format = "%Y-%m-%d %H:%M:%S"

    log.setLevel(os.getenv('INITIAL_LOG_LEVEL', logging.WARNING))
    # add last resort handler - remove later if we add syslog and/or console handler instead
    log.addHandler(logging.lastResort)
    logging.lastResort.setLevel(os.getenv('INITIAL_LOG_LEVEL', logging.WARNING))
    logging.lastResort.setFormatter(logging.Formatter(console_format, console_date_format))
    log.info(f"Setting up console and syslog logging handlers")

    if platform.platform()[:5] == "macOS":
        syslog_addr = "/var/run/syslog"
        on_mac = True
    else:
        syslog_addr = "/dev/log"
        on_mac = False

    # create handler to log to stderr;
    # skip this if running in docker on linux or service on linux to avoid double journal/docker log entries
    if on_mac or (running_in_docker == "NO" and running_as_service == "NO"):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(console_format, console_date_format))
        log.addHandler(console_handler)
        log.removeHandler(logging.lastResort)
        log.info(f"Console stderr handler added.")
    else:
        log.info(f"Running as service or in docker (not on mac) - no stderr handler added")

    if os.path.exists(syslog_addr):
        # create handler to log to syslog
        log.info(f"setting syslog to {syslog_addr} on platform {platform.platform()}")
        syslog_handler = logging.handlers.SysLogHandler(syslog_addr)
        if syslog_handler is not None:
            syslog_handler.setFormatter(logging.Formatter(syslog_format))
            log.addHandler(syslog_handler)
            log.removeHandler(logging.lastResort)
            log.info(f"Syslog handler added.")
    else:
        log.info(f"{syslog_addr} path not found - no syslog handler added")
    log.info("---------------------- Program initialize, log handlers added ------------")


def setup_logging_levels(snaptool_level, snapshots_level=logging.ERROR,
                         background_level=logging.ERROR, wekalib_level=logging.ERROR):
    log.setLevel(snaptool_level)
    log.info(" ---------------------- Setting new log levels ----------------------------")

    urllib3.add_stderr_logger(level=logging.ERROR)
    logging.getLogger("wekalib.wekacluster").setLevel(wekalib_level)
    logging.getLogger("wekalib.wekaapi").setLevel(wekalib_level)
    logging.getLogger("wekalib.sthreads").setLevel(wekalib_level)
    logging.getLogger("wekalib.circular").setLevel(wekalib_level)
    logging.getLogger("background").setLevel(background_level)
    logging.getLogger("snapshots").setLevel(snapshots_level)

def _find_config_file(configfile):
    # try to find the configfile in a couple locations if not found immediately from configfile parameter
    if os.path.exists(configfile):
        return configfile
    search_path = ['', '.', '~/', '/weka', '/opt/weka/snaptool']
    for p in search_path:
        target = os.path.join(p, configfile)
        if os.path.exists(target):
            log.info(f" * Config file found: {target}")
            return target
    log.error(f"  ***   Config file {configfile} not found")
    return configfile

def _config_parse_error(args, message):
    logging.error(f"Error in file {args.configfile}: {message} - please fix")
    time.sleep(10)

def _parse_bool(bool_str):
    if str(bool_str).lower() in ["yes", "true", "1"]:
        return True
    elif str(bool_str).lower() in ["no", "false", "0"]:
        return False
    else:
        log.error(f"Invalid boolean spec; should be 'yes', 'no', 'true' or 'false': {bool_str} in config file")
        log.error(f"Assuming False")
        return False

def _parse_check_top_level(args, config):
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
        _config_parse_error(args, msg)

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
            log.info("Attempting cluster connection...")
            self.weka_cluster = wekacluster.WekaCluster(self.clusterspec, self.authfile,
                                                        force_https=self.force_https, verify_cert=self.verify_cert)
            self.connect_datetime = now()
            connected = self.weka_cluster
        except Exception as exc:
            log.error(f"Failed to connect to cluster hosts: {self.clusterspec} with authfile: {self.authfile}.  {exc}")
        return connected

    def connection_info_different(self, new_connection):
        if (self.authfile != new_connection.authfile or
                self.clusterspec != new_connection.clusterspec or
                self.force_https != new_connection.force_https or
                self.verify_cert != new_connection.verify_cert):
            return True
        else:
            return False

    def call_weka_api(self, method, parms):
        raise_exc, err_type, errmsg = None, None, None
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
                    # After that, reconnect to cluster each failure, increase wait time.
                    # could re-read config here in case filesystem name or authfile changed
                    # or other config fixed/changed?
                    # but it will get re-read after a change/reload
                    log.warning(f"Trying reconnect to cluster before next retry.")
                    self.connect()
                    sleep_wait = 20
                i += 1
                continue
        log.error(f"call_weka_api too many failures, giving up. {method} {parms} {err_type} {errmsg}")
        raise raise_exc

    def check_cluster_connection(self):
        result = self.call_weka_api('status', {})
        log.debug(f"Cluster connected: {self.weka_cluster} io_status: {result['io_status']}")
        return True

    def create_snapshot(self, fs, name, access_point_name, upload):
        try:
            status = self.call_weka_api(method="snapshots_list", parms={'file_system': fs, 'name': name})
            if len(status) == 1:
                actions_log.info(f"Exists already: {fs} - {name}")
                return
            created_snap = self.call_weka_api(method="snapshot_create", parms={
                "file_system": fs,
                "name": name,
                "access_point": access_point_name,
                "is_writable": False})
            if created_snap is None:
                actions_log.info(f"Exists already: {fs} - {name}")
                log.info(f"   Snap {fs} {name} already exists")
            else:
                actions_log.info(f"Created {fs} - {name}")
                log.info(f"   Snap {fs}/{name} created")
            if upload:
                background.QueueOperation(self.weka_cluster, fs, name, "upload")
        except Exception as exc:
            log.error(f"Error creating snapshot {name} on filesystem {fs}: {exc}")

    def get_snapshots(self):
        snapshot_list = self.call_weka_api("snapshots_list", {})
        return snapshot_list

    def delete_old_snapshots(self, parsed_schedules_dict):
        # look at all defined schedule groups, not just last loop snaps
        # in case retentions have changed (for example, to 0)
        all_snaps = self.get_snapshots()
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
                            background.QueueOperation(self.weka_cluster, fs, s['name'], "delete")

def _exit_with_connection_status(connected):
    if connected:
        print("Connection Succeeded")
        sys.exit(0)
    else:
        print("Connection Failed")
        sys.exit(1)

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

    def print_readable(self, logger_object, level):
        msg = f"Group {self.name} (next snap: {self.next_snap_time})"
        msg += f" for filesystems {self.filesystems}, upload: {not self.no_upload}"
        logger_object.log(level, msg)
        for e in self.entries:
            logger_object.log(level, f"   {e.name}:\t{e.nextsnap_dt}\t({str(e)})")

def _log_snapgrouplist(snapgroup_list):
    [sg.print_readable(log, logging.DEBUG) for sg in snapgroup_list]

def _update_snaptimes_sorted(snapgrouplist, now_dt):
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

def get_snapgroups_for_snaptime(snapgroup_list, snaptime):
    return [item for item in snapgroup_list if item.next_snap_time == snaptime]

class SnaptoolConfig(object):
    def __init__(self, configfile, args):
        self.args = args
        self.configfile = configfile
        self.configfile_time = datetime.datetime.min
        self.config = None
        self.cluster_connection = None
        self.schedules_dict = None

    def load_config(self):
        log.debug(f"Loading config file {self.configfile}")
        try:
            with open(self.configfile, 'r') as f:
                config = yaml.load(stream=f, Loader=yaml.BaseLoader)
            log.debug(config)
            self.config = config
            self.configfile_time = os.path.getmtime(self.configfile)
        except OSError as e:
            log.error(f"Couldn't open file {self.configfile}: {e}")
            self.config = {}
        except yaml.YAMLError as y:
            log.error(f"YAML error in file {self.configfile}: {y}")
            self.config = {}
        return self.config

    def create_cluster_connection(self):
        # returns a cluster connection object
        if self.config is None:
            log.error(f"config empty or None")
            self.config = {}
        cluster_yaml = {}
        if 'cluster' in self.config:
            cluster_yaml = self.config['cluster']
        if 'hosts' in cluster_yaml:
            clusterspec = cluster_yaml['hosts']
        else:
            log.error(f"A clusterspec is required in the config file.")
            clusterspec = ''
        if 'auth_token_file' in cluster_yaml:
            authfile = cluster_yaml['auth_token_file']
        else:
            log.warning(f"No auth file specified, trying auth-token.json")
            authfile = "auth-token.json"
        if 'force_https' in cluster_yaml:
            force_https = _parse_bool(cluster_yaml['force_https'])
        else:
            force_https = False
        if 'verify_cert' in cluster_yaml:
            verify_cert = _parse_bool(cluster_yaml['verify_cert'])
        else:
            verify_cert = True
        result = ClusterConnection(clusterspec, authfile, force_https, verify_cert)
        self.cluster_connection = result
        return result

    def parse_fs_schedules(self):
        resultsdict = {}
        _parse_check_top_level(self.args, self.config)

        filesystems = self.config['filesystems']
        schedules = self.config['schedules']
        log.debug(f"JSON filesystems: {filesystems}")
        log.debug(f"JSON schedules: {schedules}")
        for schedname, schedule_spec in schedules.items():
            new_group = ScheduleGroup(schedname)
            resultsdict[schedname] = new_group
            if "every" in schedule_spec.keys():  # single schedule item without a sub-schedule name
                entry = snapshots.parse_schedule_entry(None, schedname, schedule_spec)
                if entry is not None:
                    new_group.entries.append(entry)
            else:
                for schedentryname, schedentryspec in schedule_spec.items():
                    entry = snapshots.parse_schedule_entry(schedname, schedentryname, schedentryspec)
                    if entry is not None:
                        new_group.entries.append(entry)
        for fs_name, fs_schedulegroups in filesystems.items():
            if isinstance(fs_schedulegroups, str):
                fs_schedulegroups = snapshots.comma_string_to_list(fs_schedulegroups)
                filesystems[fs_name] = fs_schedulegroups
            log.info(f"{fs_name}, {fs_schedulegroups}")
            for sched_name in fs_schedulegroups:
                if sched_name not in resultsdict.keys():
                    _config_parse_error(self.args, f"Schedule {sched_name}, listed for filesystem {fs_name}, not found")
                else:
                    resultsdict[sched_name].filesystems.append(fs_name)
        self.schedules_dict = resultsdict
        return resultsdict

    def reload(self):
        if not os.path.exists(self.configfile):
            log.error(f"Config file {self.configfile} missing.")
            return False
        self.configfile_time = os.path.getmtime(self.configfile)
        log.info(f"--------------- Reloading configuration file {self.configfile}")
        try:
            new_stc = SnaptoolConfig(self.configfile, self.args)
            new_stc.load_config()
            new_connection = new_stc.create_cluster_connection()
            new_schedules_dict = new_stc.parse_fs_schedules()
            if self.cluster_connection.connection_info_different(new_connection):
                log.info(f"-------------------- Reconnecting with new cluster configuration...")
                connected = new_connection.connect()
                if connected:
                    self.config = new_stc.config
                    self.cluster_connection = new_connection
                    self.schedules_dict = new_schedules_dict
                    return connected
                else:
                    log.error(f"--------------------    Reconnection failed; using existing config info.")
                    return False
            else:
                self.schedules_dict = new_schedules_dict
                return True
        except Exception as e:
            log.error(f"--------------------    Reload error for {self.configfile}; using existing config info. {e}")
            return False

    def sleep_with_reloads(self, num_seconds, check_interval_seconds):
        sleep_time_left = num_seconds
        while sleep_time_left > 0:
            sleep_time = min(check_interval_seconds, sleep_time_left)
            sleep_time_left -= sleep_time
            time.sleep(sleep_time)
            if not os.path.exists(self.configfile):
                log.error(f"Config file {self.configfile} missing.")
                return False
            elif os.path.getmtime(self.configfile) > self.configfile_time:
                use_new_config = self.reload()
                if use_new_config:
                    return True
        return False

    def next_snaps(self):
        sg_list = list(self.schedules_dict.values())
        _update_snaptimes_sorted(sg_list, now())
        _log_snapgrouplist(sg_list)
        next_snap_time = sg_list[0].entries[0].nextsnap_dt  # because it's sorted this works
        snapgroups_for_nextsnap = get_snapgroups_for_snaptime(sg_list, next_snap_time)
        log.debug(f"next snap time: {next_snap_time}, {len(snapgroups_for_nextsnap)} snaps")
        next_snaps_dict = get_snaps_dict_by_fs(snapgroups_for_nextsnap, next_snap_time)
        sleep_time_left = round((next_snap_time - now()).total_seconds(), 1)
        snaps_msg_str = f"{ {fs: s.name for fs, s in next_snaps_dict.items()} }"

        if sleep_time_left <= 0:
            sleep_msg = f"Snap now: ({next_snap_time}): {snaps_msg_str}"
            sleep_time_left = 0
        else:
            sleep_msg = f"Sleep until {next_snap_time} ({sleep_time_left}s), then snap: {snaps_msg_str}"
        log.info(sleep_msg)
        return next_snap_time, next_snaps_dict, sleep_time_left

    def create_new_snapshots(self, next_snaps_dict, next_snap_time):
        for fs, snap in next_snaps_dict.items():
            access_point_name = next_snap_time.astimezone(timezone.utc).strftime(
                "@GMT-%Y.%m.%d-%H.%M.%S")  # windows format
            # don't need century, or date at all really, because snap creation time is used for delete, comparisons
            # date in the name is really for convenience in displays
            next_snap_name = snap.name + "." + next_snap_time.strftime("%y%m%d%H%M")
            log.info(f"Creating fs/snap {fs}/{next_snap_name} (name len={len(next_snap_name)})")
            self.cluster_connection.create_snapshot(fs, next_snap_name, access_point_name, snap.upload)

    def delete_old_snapshots(self):
        self.cluster_connection.delete_old_snapshots(self.schedules_dict)

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

def main():
    connected = False

    args, loglevel = parse_snaptool_args()
    setup_logging_initial()
    # handle signals (ie: ^C and such)
    signals.signal_handling()
    setup_logging_levels(loglevel, snapshots_level=loglevel, background_level=loglevel)
    log.info(f"Version info: {version_string()}")
    snapshots.run_schedule_tests()    # scheduling computation self tests for snapshots module

    snaptool_config = SnaptoolConfig(args.configfile, args)

    while not connected:
        snaptool_config.load_config()
        snaptool_config.create_cluster_connection()
        connected = snaptool_config.cluster_connection.connect()
        if args.test_connection_only:
            _exit_with_connection_status(connected)
        if not connected:
            log.error(f"Connection to {snaptool_config.cluster_connection.clusterspec} failed.  "
                      f"Sleeping for a minute, then reloading config and trying again.")
            time.sleep(60)

    setup_actions_log()
    log.warning("Initializing background q and replaying operation intent log...")
    background.init_background_q()
    background.intent_log.replay(snaptool_config.cluster_connection.weka_cluster)

    snaptool_config.schedules_dict = snaptool_config.parse_fs_schedules()

    reload_interval = 30

    while True:
        # delete is before and after create in the loop to make sure we utilize sleep time for deletes
        snaptool_config.delete_old_snapshots()

        next_snap_time, next_snaps_dict, sleep_time_left = snaptool_config.next_snaps()
        new_config_loaded = snaptool_config.sleep_with_reloads(sleep_time_left, reload_interval)
        if new_config_loaded:
            continue

        snaptool_config.create_new_snapshots(next_snaps_dict, next_snap_time)
        snaptool_config.delete_old_snapshots()

        additional_sleep_time = 60 - round((now() - next_snap_time).total_seconds(), 1)
        # if it has been less than a minute since the last snaps were created, wait til top of the minute
        if additional_sleep_time > 0:
            log.info(f"Sleeping for {additional_sleep_time} seconds before next loop")
            if snaptool_config.sleep_with_reloads(additional_sleep_time, reload_interval):
                time.sleep(additional_sleep_time - reload_interval)


if __name__ == '__main__':
    main()
