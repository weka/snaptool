
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
# import importlib_metadata as importmeta

from wekalib import __version__ 
WEKALIBVERSION = __version__

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
import flask_ui
from contextlib import contextmanager

VERSION = "1.6.1"

# get the root logger, get snaptool logger
log = logging.getLogger()
actions_log = logging.getLogger("snapshot_actions_log")
actions_log_file = "snaptool.log"
actions_log_resolved_file = None

running_in_docker = os.getenv('IN_DOCKER_CONTAINER', 'NO')
running_as_service = os.getenv('LAUNCHED_BY_SYSTEMD', 'NO')

@contextmanager
def logging_level(level):
    old_level = log.getEffectiveLevel()
    log.setLevel(level)
    try:
        yield
    finally:
        log.setLevel(old_level)

def version_string():
    return (f"{sys.argv[0]} version: {VERSION}"
            f" wekalib-version={WEKALIBVERSION}"
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
    argparser.add_argument("-p", "--http-port", dest="http_port", default=8090,
                            help="http port to use for status ui webserver.   Use 0 to disable")
    argparser.add_argument("--access-point-format", dest="access_point_format", default="@GMT-%Y.%m.%d-%H.%M.%S",
                            # help="format for access point name.  Default format supports Windows Previous versions.  If using SMB you probably shouldn't change this."
                            help=argparse.SUPPRESS
                            )
    argparser.add_argument("--retain-max", dest="retain_max", default=365, type=int,
                            # help="max value for schedule 'retain'"
                            help=argparse.SUPPRESS
                            )
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

def check_other_snaptool_args(args):
    rmin = snapshots.RETAIN_MIN
    rlim = snapshots.RETAIN_LIMIT
    rmax = snapshots.RETAIN_MAX
    if args.retain_max >= rmin and args.retain_max <= rlim:
        if  args.retain_max != rmax:
            snapshots.RETAIN_MAX = args.retain_max
            with logging_level(logging.INFO):
                log.info(f"Hidden arg retain-max set to non-standard {snapshots.RETAIN_MAX}")
    else:
        log.error(f"Ignoring invalid retain-max arg ({args.retain_max}).  Valid range is [{rmin},{rlim}]; using default ({rmax})")

def now():
    return datetime.datetime.now()

def setup_actions_log():
    log.info(f"Setting up actions log {actions_log_file}")
    resolved_fname = background.create_log_dir_file(actions_log_file)
    log.info(f"actions log file: {resolved_fname}")
    global actions_log_resolved_file
    actions_log_resolved_file = resolved_fname

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
        if syslog_handler != None:
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
        return os.path.abspath(configfile)
    search_path = ['', '.', '~/', '/weka', '/opt/weka/snaptool']
    for p in search_path:
        target = os.path.join(p, configfile)
        if os.path.exists(target):
            log.info(f" * Config file found: {target}")
            return os.path.abspath(target)
    log.error(f"  ***   Config file {configfile} not found")
    return configfile

def _config_parse_error(args, message):
    logging.error(f"Error in file {args.configfile}: {message} - please fix")

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
    if 'snaptool' in config:
        msg += f"'snaptool' section found."
    else:
        msg += f"'snaptool' section not found."
    log.info(f"Config parse: {msg}")
    if s_found and fs_found and c_found:
        log.info(f"Config top level check ok. {msg}")
    else:
        _config_parse_error(args, msg)

class ClusterConnection(object):
    def __init__(self, clusterspec, authfile, force_https, cert_check):
        self.weka_cluster = None
        self.weka_cluster_name = ""
        self.clusterspec = clusterspec
        self.authfile = authfile
        self.force_https = force_https
        self.verify_cert = cert_check
        self.connected_since = datetime.datetime.max

    def connect(self):
        connected = False
        msg = ""
        try:
            log.info("Attempting cluster connection...")
            self.weka_cluster = wekacluster.WekaCluster(self.clusterspec, self.authfile,
                                                        force_https=self.force_https, 
                                                        verify_cert=self.verify_cert)
            self.authfile = self.weka_cluster.authfile
            self.weka_cluster_name = self.weka_cluster.name
            self.connected_since = now()
            connected = self.weka_cluster
        except Exception as exc:
            msg = f"Connection to cluster hosts '{self.clusterspec}'"
            msg += f" failed with authfile '{self.authfile}' - {exc}"
            log.error(msg)
        return connected, msg

    def connection_info_different(self, new_connection):
        checks = [self.authfile != new_connection.authfile,
                    self.clusterspec != new_connection.clusterspec,
                    self.force_https != new_connection.force_https,
                    self.verify_cert != new_connection.verify_cert]
        if any(checks):
            log.info(f"Connection_info_different checks: {checks}")
            return True
        else:
            return False

    def call_weka_api(self, method, parms, max_tries=20):
        raise_exc, err_type, errmsg = None, None, None
        sleep_wait = 5
        for i in range(max_tries):
            try:
                log.debug(f"calling api {method} with {parms}")
                result = self.weka_cluster.call_api(method, parms)
                log.debug(f"api call {method} with {parms} returned: {result}")
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
                log.warning(f"Will try again after {sleep_wait} seconds (retry {i + 1} of {max_tries})...")
                time.sleep(sleep_wait)
                if i >= 2:  # first couple times just wait and try again.
                    # After that, reconnect to cluster each failure, increase wait time.
                    # could re-read config here in case filesystem name or authfile changed
                    # or other config fixed/changed?
                    # but it will get re-read after a change/reload
                    connected, msg = self.connect()
                    log.warning(f"Tried reconnect to cluster before retry.  Result: {connected} {msg}.")
                    sleep_wait = 20
                i += 1
                continue
            except Exception as exc:
                raise_exc = exc
                log.error(f"call_weka_api on cluster {self.weka_cluster} failed for {method}: {exc}")
        m = f"call_weka_api too many failures, giving up. {method} {parms} {err_type} {errmsg} {msg}"
        log.error(m)
        raise raise_exc

    def check_cluster_connection(self):
        result = self.call_weka_api('status', {})
        log.debug(f"Cluster connected: {self.weka_cluster} io_status: {result['io_status']}")
        return True

    def create_snapshot(self, fs, name, access_point_name, upload):
        try:
            status = self.call_weka_api(method="snapshots_list", parms={'file_system': fs, 'name': name})
            if len(status) == 1:
                actions_log.info(f"Snapshot exists: {fs} - {name}")
                return
            created_snap = self.call_weka_api(method="snapshot_create", parms={
                "file_system": fs,
                "name": name,
                "access_point": access_point_name,
                "is_writable": False})
            if created_snap == None:
                actions_log.info(f"Snapshot exists: {fs} - {name}")
                log.info(f"   Snap {fs}/{name} already exists")
            else:
                actions_log.info(f"Created snap {fs} - {name}")
                log.info(f"   Snap {fs}/{name} created")
            upload_op = False
            if upload == True or str(upload).upper() == 'LOCAL':
                upload_op = "upload"
            elif str(upload).upper() == 'REMOTE':
                upload_op = "upload-remote"
            if upload_op:
                background.QueueOperation(self.weka_cluster, fs, name, upload_op)
        except Exception as exc:
            log.error(f"Error creating snapshot {name} on filesystem {fs}: {exc}")

    def get_snapshots(self):
        snapshot_list = self.call_weka_api("snapshots_list", {})
        if isinstance(snapshot_list, dict):
            snapshot_list = list(snapshot_list.values())
        log.debug(f"get_snapshots: {[s['name'] for s in snapshot_list]}")
        return snapshot_list

    def delete_old_snapshots(self, parsed_schedules_dict):
        # look at all defined schedule groups, not just last loop snaps
        # in case retentions have changed (for example, to 0)
        all_snaps = self.get_snapshots()
        sg_list = list(parsed_schedules_dict.values())
        for sg in sg_list:
            for entry in sg.entries:
                for fs in sg.filesystems:
                    snaps = get_fs_snaps(all_snaps, fs, entry.name)
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

def get_file_mtime(path):
    mtimeos = os.path.getmtime(path)
    return datetime.datetime.fromtimestamp(mtimeos)

class SnaptoolConfig(object):
    def __init__(self, configfile, args):
        self.args = args
        self.configfile = configfile
        self.configfile_time = datetime.datetime.min
        self.config = None
        self.cluster_connection = None
        self.schedules_dict = None
        self.schedules_dict_unused = {}
        self.schedules_dict_used = {}       # if a fs references it
        self.errors = []
        self.ignored_errors = []
        self.resolved_actions_log = None
        self.next_snap_time = datetime.datetime.now()
        self.next_snaps_dict = {}
        self.background_progress_message = ""
        self.flask_http_port = 8090
        self.obs_list = []
        self.filesystems = []

    def load_config(self):
        log.debug(f"Loading config file {self.configfile}")
        try:
            with open(self.configfile, 'r') as f:
                config = yaml.load(stream=f, Loader=yaml.BaseLoader)
            log.debug(config)
            self.config = config
            self.configfile_time = get_file_mtime(self.configfile)
        except OSError as e:
            m = f"Couldn't open file {self.configfile}: {e}"
            log.error(m)
            self.config = {}
        except yaml.YAMLError as y:
            m = f"YAML read error in file {self.configfile}: {y}"
            log.error(m)
            self.config = {}
        return self.config

    def create_cluster_connection(self):
        # returns a cluster connection object
        if self.config == None:
            log.error(f"config empty or None")
            self.config = {}
        cluster_yaml = {}
        if 'cluster' in self.config:
            cluster_yaml = self.config['cluster']
        if 'hosts' in cluster_yaml:
            clusterspec = cluster_yaml['hosts']
        else:
            m = f"A clusterspec is required in the config file."
            background.background_q.message(m)
            log.error(m)
            clusterspec = ''
        if 'auth_token_file' in cluster_yaml:
            authfile = cluster_yaml['auth_token_file']
        else:
            m = f"No auth file specified, trying auth-token.json"
            log.warning(m)
            background.background_q.message(m)
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
        return result

    def parse_snaptool_settings(self):
        p = 8090
        h = '0.0.0.0'
        if 'snaptool' in self.config:
            st = self.config['snaptool']
            if 'port' in st:
                p = st['port']
                log.info(f"from config file - snaptool.port = {p}")
            if 'host' in st:
                h = st['host']
                log.info(f"from config file - snaptool.host = {h}")
        self.flask_http_port = int(p)
        return p, h

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
                entry, err_reason = snapshots.parse_schedule_entry(None, schedname, schedule_spec)
                if entry != None:
                    new_group.entries.append(entry)
                else:
                    self.ignored_errors.append(err_reason)
            else:
                for schedentryname, schedentryspec in schedule_spec.items():
                    entry, err_reason = snapshots.parse_schedule_entry(schedname, schedentryname, schedentryspec)
                    if entry != None:
                        new_group.entries.append(entry)
                    else:
                        self.ignored_errors.append(err_reason)
        for fs_name, fs_schedulegroups in filesystems.items():
            if isinstance(fs_schedulegroups, str):
                fs_schedulegroups = snapshots.comma_string_to_list(fs_schedulegroups)
                filesystems[fs_name] = fs_schedulegroups
            log.info(f"{fs_name}, {fs_schedulegroups}")
            for sched_name in fs_schedulegroups:
                if sched_name not in resultsdict.keys():
                    self.ignored_errors.append(f"Schedule '{sched_name}' is listed for filesystem {fs_name} but not defined")
                    _config_parse_error(self.args, f"Schedule '{sched_name}', listed for filesystem {fs_name}, not found")
                else:
                    resultsdict[sched_name].filesystems.append(fs_name)
        self.schedules_dict_unused = {k:v for k,v in resultsdict.items() if not v.filesystems}
        self.schedules_dict_used = {k:v for k,v in resultsdict.items() if v.filesystems}
        self.schedules_dict = {**self.schedules_dict_used, **self.schedules_dict_unused}
        return resultsdict
    
    def update_schedule_changes(self, new_schedules, new_unused, new_used, new_ignored, new_errors):
        self.schedules_dict = new_schedules
        self.schedules_dict_unused = new_unused
        self.schedules_dict_used = new_used
        self.ignored_errors = new_ignored
        self.errors = new_errors

    def reload(self, always_reconnect=False):
        if not os.path.exists(self.configfile):
            m = f"Config file {self.configfile} missing."
            log.error(m)
            self.errors.append(m)
            return False, False
        log.info(f"--------------- (Re)loading configuration file {self.configfile}")
        try:
            self.configfile_time = get_file_mtime(self.configfile)
            new_stc = SnaptoolConfig(self.configfile, self.args)
            new_stc.load_config()
            new_stc.parse_snaptool_settings()
            if new_stc.flask_http_port != self.flask_http_port:
                if new_stc.flask_http_port != 0:
                    log.info(f"(Re)tarting ui from reload...")
                    stop_ui()
                    log.info(f"ui stopped for reload...")
                    self.flask_http_port = new_stc.flask_http_port
                    log.info(f"(Re)tarting ui from reload on new port {self.flask_http_port}...")
                    maybe_start_ui(self)
                else:
                    log.info(f"Stopping ui from reload...")
                    stop_ui()
                    self.flask_http_port = new_stc.flask_http_port
            new_stc.parse_fs_schedules()
            new_connection = new_stc.create_cluster_connection()
            if not self.config:
                self.config = new_stc.config
                self.cluster_connection = new_connection
                self.update_schedule_changes(new_stc.schedules_dict,
                        new_stc.schedules_dict_unused, new_stc.schedules_dict_used, 
                        new_stc.ignored_errors, new_stc.errors)
            if always_reconnect or not self.cluster_connection or self.cluster_connection.connection_info_different(new_connection):
                log.info(f"-------------------- (Re)connecting with new cluster configuration...")
                connected, msg = new_connection.connect()
                log.info(f"-------------------- connect returned: {connected} {msg}")
                if connected:
                    self.errors = []
                    self.config = new_stc.config
                    self.update_schedule_changes(new_stc.schedules_dict,
                        new_stc.schedules_dict_unused, new_stc.schedules_dict_used, 
                        new_stc.ignored_errors, new_stc.errors)
                    self.cluster_connection = new_connection
                    return connected, True
                else:
                    m = f"Connection attempt failed; using stored connection info.  error: {msg}"
                    self.errors.append(m)
                    log.error(f"--------------------    {m}")
                    return connected, True
            else:
                log.info(f"--------------------   No cluster connection changes to config file since last good connect.")
                self.update_schedule_changes(new_stc.schedules_dict,
                    new_stc.schedules_dict_unused, new_stc.schedules_dict_used, 
                    new_stc.ignored_errors, new_stc.errors)
                return True, True
        except Exception as e:
            m = f"Reload error for {self.configfile}; using existing config info. {e}"
            self.errors.append(m)
            log.error(f"--------------------    {m}")
            return False, True

    def sleep_with_reloads(self, num_seconds, check_interval_seconds):
        sleep_time_left = num_seconds
        while sleep_time_left > 0:
            sleep_time = min(check_interval_seconds, sleep_time_left)
            sleep_time_left -= sleep_time
            time.sleep(sleep_time)
            if not os.path.exists(self.configfile):
                m = f"Config file {self.configfile} missing."
                log.error(m)
                self.errors.append(m)
                return False
            elif get_file_mtime(self.configfile) > self.configfile_time:
                use_new_config, _ = self.reload()
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
        background.background_q.message(sleep_msg)
        self.next_snap_time = next_snap_time
        self.next_snaps_dict = next_snaps_dict
        return next_snap_time, next_snaps_dict, sleep_time_left

    def create_new_snapshots(self, next_snaps_dict, next_snap_time):
        for fs, snap in next_snaps_dict.items():
            format = self.args.access_point_format
            access_point_name = next_snap_time.astimezone(timezone.utc).strftime(format)
            # default is     "@GMT-%Y.%m.%d-%H.%M.%S"  # used to support windows previous versions
            # don't need century, or date at all really, because snap creation time is used for delete and comparisons
            # date in the name is really for convenience in displays
            # allowed substitions
            # single % from strftime (%y, %m, etc)
            # %%name - replaced with snapshot definition name (doesn't include the added date)
            # %%fs - replaced with filesystem name
            # otherwise this is standard strftime format
            access_point_name = access_point_name.replace("%name", snap.name)
            access_point_name = access_point_name.replace("%fs", fs)
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
    # <something> has to be a 10 digit string of digits (looks like yymmddhhmm)
    log.debug(f"Getting snaps specific to {fs} and {schedname}")
    snaps_for_fs = []
    for s in all_snaps:
        snap_name = s['name'].split('.')
        if s['filesystem'] == fs and len(snap_name) == 2 and snap_name[0] == schedname \
                                 and snap_name[1].isdigit() and len(snap_name[1]) == 10:
            snaps_for_fs.append(s)
    snaps_for_fs.sort(key=itemgetter('creationTime'))
    return snaps_for_fs

def maybe_start_ui(snaptool_config):
    if flask_ui.sconfig == None and snaptool_config.flask_http_port != 0:
        flask_ui.run_ui(snaptool_config)

def stop_ui():
    flask_ui.stop_ui()
    time.sleep(5)
    flask_ui.sconfig = None

def main():
    connect_succeeded = False

    args, loglevel = parse_snaptool_args()
    setup_logging_initial()
    # handle signals (ie: ^C and such)
    signals.signal_handling()
    setup_logging_levels(loglevel, snapshots_level=loglevel, background_level=loglevel)
    log.info(f"Version info: {version_string()}")
    
    check_other_snaptool_args(args)
    
    # run scheduling computation self tests for snapshots module
    # but don't raise errors for expected failures 
    snapshots.run_schedule_tests(raise_expected_errors=False)    

    snaptool_config = SnaptoolConfig(args.configfile, args)

    if not args.test_connection_only:
        setup_actions_log()
        snaptool_config.resolved_actions_log = actions_log_resolved_file
        m = "Initializing background q and replaying operation intent log..."
        log.info(m)
        background.background_q.message(m)
        background.init_background_q()

    if args.http_port != 0:
        snaptool_config.flask_http_port = args.http_port
    maybe_start_ui(snaptool_config)

    while not connect_succeeded:
        connect_succeeded, config_found = snaptool_config.reload(always_reconnect=True)
        log.info(f"Config reload results: {connect_succeeded} {config_found}")
        if args.test_connection_only:
            _exit_with_connection_status(connect_succeeded)
        if not connect_succeeded:
            if config_found and snaptool_config.cluster_connection:
                cl = snaptool_config.cluster_connection.clusterspec
                au = snaptool_config.cluster_connection.authfile
                cerror = f"Connection to {cl} with authfile {au} failed.  " \
                            f"Sleeping, then reloading config and trying again."
            elif config_found:
                cerror = f"Config found but no cluster_connection"
            else:
                cerror = f"Snaptool configuration file {args.configfile} not found"
            background.background_q.message(cerror)
            log.info(cerror)
            time.sleep(15)
        else:
            background.background_q.message("Connected to cluster")
            
    background.intent_log.replay(snaptool_config.cluster_connection.weka_cluster)

    try:
        fs_list = snaptool_config.cluster_connection.call_weka_api("filesystems_list", {})
        
        if isinstance(fs_list, list):
            for fs in fs_list:
                msg = f"fs {fs['name']}: obs_buckets: {fs['obs_buckets']}"
                print(msg)
                log.info(msg)
        else:
            for fsid, fsdict in fs_list.items():
                msg = f"fs {fsdict['name']}: obs_buckets: {fsdict['obs_buckets']}"
                print(msg)
                log.info(msg)
    except Exception as exc:
        log.error(f"Error getting obs_s3_list or filesystems info: {exc}")
   
    reload_interval = 15

    while True:
        # delete is before and after create in the loop to make sure we utilize sleep time for deletes
        snaptool_config.delete_old_snapshots()

        next_snap_time, next_snaps_dict, sleep_time_left = snaptool_config.next_snaps()
        new_config_loaded = snaptool_config.sleep_with_reloads(sleep_time_left, reload_interval)
        if new_config_loaded:
            continue

        snaptool_config.create_new_snapshots(next_snaps_dict, next_snap_time)

        additional_sleep_time = 60 - round((now() - next_snap_time).total_seconds(), 1)
        # if it has been less than a minute since the last snaps were created, wait til top of the minute
        if additional_sleep_time > 0:
            snaptool_config.delete_old_snapshots()
            log.info(f"Sleeping for {additional_sleep_time} seconds before next loop")
            if snaptool_config.sleep_with_reloads(additional_sleep_time, reload_interval):
                time.sleep(additional_sleep_time - reload_interval)


if __name__ == '__main__':
    main()
