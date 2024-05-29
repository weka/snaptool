#!/usr/bin/env python3

# background.py - manage snapshot uploads
# Vince Fleming
# vince@weka.io
#
# cleanup for new scheduling and getting rid of warnings - Bruce Clagett bruce@weka.io

# system imports
import os
import queue
from collections import deque
import threading
import time
import uuid
import logging
import string
import datetime
import pandas as pd

logdir = "logs"
log = logging.getLogger(__name__)
actions_log = logging.getLogger("snapshot_actions_log")
intent_log_filename = "snap_intent_q.log"
intent_log = 'Global uninitialized'


class UploadDownloadQueue(queue.Queue):
    def __init__(self):
        self.progress_messages = deque(maxlen=500)
        self.progress_messages.append("Initializing/waiting...")
        self.locators = {}
        queue.Queue.__init__(self)

    def message(self, messagestr):
        t = f"{datetime.datetime.now()}"[:19]
        m = f"{t} {messagestr}"
        log.info(m)
        self.progress_messages.append(m)

background_q = UploadDownloadQueue()

def create_log_dir_file(filename):
    prevmask = os.umask(0)
    if not os.path.isdir(logdir):
        os.mkdir(logdir, mode=0o777)
    else:
        os.chmod(logdir, 0o777)
    fname = f"{logdir}/{filename}"
    if not os.path.isfile(fname):
        with open(fname, 'w'):
            log.info(f"Created file {fname}")
    os.chmod(fname, 0o666)
    os.umask(prevmask)
    return fname

class IntentLog(object):
    def __init__(self, logfilename):
        self._lock = threading.Lock()
        self.filename = create_log_dir_file(logfilename)

    def rotate(self):  # only rotate if needed
        with self._lock:
            file_stat = os.stat(self.filename)
            filesize = file_stat.st_size

            if filesize > 1024 * 1024:
                # move file to .1
                if os.path.exists(self.filename + '.1'):
                    os.remove(self.filename + '.1')
                os.rename(self.filename, self.filename + '.1')

    # append a record
    def put_record(self, uuid_s, fsname, snapname, snap_op, status, dt='now', loc='', bucket=''):
        if dt == 'now':
            dt = datetime.datetime.now().strftime("%Y%m%d.%H%M%S.%f")
        with self._lock:
            with open(self.filename, "a") as fd:
                fd.write(f"{uuid_s}:{fsname}:{snapname}:{snap_op}:{status}:{dt}:{loc}:{bucket}\n")

    # replay the log on a cluster
    def replay(self, cluster):
        log.info(f"Replaying background intent log")
        log.info(f"running undeleted locators processing...")
        self.cleanup_intent_log(cluster)
        undeleted = self.get_records_pd()
        log.info(f"finished undeleted processing; len={len(undeleted)}")
        replay_start = time.time()
        for uuid_str, fsname, snapname, snap_op in self._incomplete_records():
            log.info(f"re-scheduling {fsname}/{snapname} for {snap_op}")
            QueueOperation(cluster, fsname, snapname, snap_op, uuid_str=uuid_str)
        replay_elapsed_ms = round((time.time() - replay_start) * 1000, 1)
        log.warning(f"Replay intent log took {replay_elapsed_ms} ms")

    # yield back all records - returns uuid, fsname, snapname, operation, status
    def _records(self):
        with self._lock:
            for filename in [self.filename + '.1', self.filename]:
                try:
                    with open(filename, "r") as fd:
                        for record in fd:
                            record = record.split('\n')[0] # remove newline
                            temp = record.split(':')
                            if len(temp) not in (5, 8):
                                log.error(f"Invalid record in intent log: {record}")
                                continue
                            uid, fs, name, op, status = temp[0:5]
                            if len(temp) == 5:
                                dt = name.split(".",-1)[1]
                                loc, bucket = '', ''
                            if len(temp) == 8:
                                dt = temp[5]
                                loc = temp[6]
                                bucket = temp[7]
                            yield temp[0], temp[1], name, temp[3], temp[4], dt, loc, bucket                                 
                except FileNotFoundError:
                    log.info(f"Log file {filename} not found")
                    continue

    # un-completed records - an iterable
    def _incomplete_records(self):
        snaps = {}
        # distill the records to just ones that need to be re-processed
        log.info("Reading intent log")
        for uid, fsname, snapname, cluster_op, status, dt, loc, bucket in self._records():
            if uid not in snaps:
                snapshot = dict()
                snapshot['fsname'] = fsname
                snapshot['snapname'] = snapname
                snapshot['operation'] = cluster_op
                snapshot['status'] = status
                snapshot['uid'] = uid
                if status != "complete":     # first encounter in file is complete - unlikely but with rotations possible
                    snaps[uid] = snapshot
            else:
                if status == "complete":
                    log.debug(f"De-queuing snap {uid} {fsname}/{snapname} (complete)")
                    del snaps[uid]  # remove ones that completed so we don't need to look through them
                else:
                    log.debug(f"Updating status of snap {uid} {fsname}/{snapname} to {status}")
                    snaps[uid]['status'] = status  # update status

        # this should be a very short list - far less than 100; likely under 5
        grouped_snaps = {"queued": {}, "in-progress": {}, "error": {}, "complete": {}}

        for uid, snapshot in snaps.items():  # grouped so we can see in-progress and error first
            log.debug(f"uuid={uid}, snapshot={snapshot}")
            grouped_snaps[snapshot['status']][uid] = snapshot

        if len(grouped_snaps['complete']) != 0:
            log.error(f"Error in _incomplete_records - completed item found after grouping: {grouped_snaps['complete']}")
        
        log.debug(f"sorted_snaps = {grouped_snaps}")
        log.debug(
            f"There are {len(grouped_snaps['error'])} error snaps, {len(grouped_snaps['in-progress'])} in-progress"
            f" snaps, and {len(grouped_snaps['queued'])} queued snaps in the intent log")

        log.info(f"intent-log incomplete records len: {len(snaps)}")

        # process in order of status # not sure about error ones... do we re-queue?  Should only be 1 in-progress too
        for status in ["in-progress", "error", "queued"]:
            for uid, snapshot in grouped_snaps[status].items():
                # these should be re-queued because they didn't finish or got error
                log.debug(f"re-queueing snapshot = {snapshot}, status={status}")
                yield uid, snapshot['fsname'], snapshot['snapname'], snapshot['operation']

    def get_records_pd(self):
        names = ['uid', 'fs', 'snapname', 'op', 'status', 'dt', 'loc', 'bucketname']
        with self._lock:
            df = pd.read_csv(self.filename, sep=':', names=names)
        log.info(df.count())
        complete = df.loc[df['status'] == 'complete']
        log.info(f"complete count: {len(complete)}")
        withloc_complete = complete.dropna()
        log.info(f"complete notna count: {len(withloc_complete)}")
        result = withloc_complete.sort_values(by=['fs','snapname','dt'])
        result = result.drop_duplicates(keep='last', subset=['fs','snapname','op'])
        groupbycols = ['fs','snapname','loc','bucketname']
        result = result.groupby(groupbycols, as_index=False).agg({'op': '-'.join})
        result = result[result.op != 'upload-delete']
        result_local = result[result.op == 'upload']
        result_remote = result[result.op == 'upload-remote']
        result_remote_deleted = result[result.op == 'upload-remote-delete']
        log.info(f"withloc complete sorted list count: {len(result)}")

        return [result_local.to_dict('records'), 
                result_remote.to_dict('records'), 
                result_remote_deleted.to_dict('records')]

    def get_snapshots(self, cluster):
        if cluster:
            try:  # doesn't retry - intended for quick updates when properly connected
                all_snaps = cluster.call_api(method="snapshots_list", parms={})
                return all_snaps
            except Exception as exc:
                log.info(f"Error getting snapshots list: {exc}")
                return []

    def cleanup_intent_log(self, cluster):
        # if there are any deleted local snapshots that aren't marked deleted, mark them
        if cluster:
            all_snaps = self.get_snapshots(cluster)
            if all_snaps and len(all_snaps) > 0:    # only do cleanup if we're sure we have a connection
                log.info(f"cluster all_snaps: {len(all_snaps)}")
                local, remote, _ = self.get_records_pd()
                for l in local + remote:
                    fs, snap = l['fs'], l['snapname']
                    for s in all_snaps:
                        if (s['name'] == l['snapname']) and (s['filesystem'] == l['fs']):
                            break
                    else:
                        lloc, bn= l['loc'], l['bucketname']
                        log.info(f"Queueing {l['fs']} {l['snapname']} for delete")
                        QueueOperation(cluster, fs, snap, 'delete', loc=lloc, bucket=bn)

base_62_digits = string.digits + string.ascii_uppercase + string.ascii_lowercase

def int_to_base_62(num: int):
    # only works for numbers > 0, which all uuid ints should be.
    assert num > 0, f"int_to_base_n: num ({num}) must be greater than 0"
    result = ''
    base = len(base_62_digits)
    while num > 0:
        num, remainder = divmod(num, base)
        result = base_62_digits[remainder] + result
    return result

def get_short_unique_id():     # returns a uuid4 that has been converted to base 62
    number = uuid.uuid4().int
    result = int_to_base_62(number)
    if len(result) < 21:        # prefer a consistent length and most are 22 chars long, fill to 22
        result.zfill(22)
    return result

class QueueOperation(object):
    def __init__(self, cluster, fsname, snapname, op, loc='', bucket='', uuid_str=None, dt='now'):
        global background_q
        global intent_log

        self.fsname = fsname
        self.snapname = snapname
        self.operation = op
        self.cluster = cluster
        self.loc = loc
        self.bucket = bucket
        if dt == 'now':
            dt = datetime.datetime.now().strftime("%Y%m%d.%H%M%S.%f")
        self.dt = dt

        if uuid_str == None:
            uuid_str = get_short_unique_id()
        self.uuid = uuid_str

        # queue the request
        if op == "delete":
            for i in list(background_q.queue):
                if i.fsname == fsname and i.snapname == snapname and i.operation == "delete":
                    log.debug(f"duplicate delete for {fsname}/{snapname} {op} ignored")
                    return           # already in the queue, don't queue again for deletes
        if fsname != "WEKA_TERMINATE_THREAD" and snapname != "WEKA_TERMINATE_THREAD":
            intent_log.put_record(self.uuid, fsname, snapname, op, "queued")
        background_q.put(self)

    def get_html(self):
        return f"{self.operation} {self.fsname}/{self.snapname}"

# process operations in the background - runs in a thread - starts before replaying log
def background_processor():
    global background_q      # queue of QueueOperation objects
    global intent_log       # log file(s) for all QueueOperation objects created, for replay if necessary

    def snapshot_status(q_snap_obj):
        fsname = q_snap_obj.fsname
        snapname = q_snap_obj.snapname
        cluster = q_snap_obj.cluster
        # get snap info via api - assumes snap has been created already
        status = []
        for i in range(3):   # try 3 times on some errors
            try:
                status = cluster.call_api(method="snapshots_list",
                                            parms={'file_system': fsname, 'name': snapname})
            except Exception as exc:
                log.error(f"Error getting snapshot status for {fsname}/{snapname}: {exc}")
                if "(502) Bad Gateway" in str(exc):
                    # pause and try again
                    time.sleep(5)
                    continue
                else:
                    raise  # API error - let calling routine handle it
        
        if len(status) == 0:
            # hmm... this one doesn't exist on the cluster? Let calling routine handle it
            # might be on purpose, or checking that it got deleted
            return None
        elif len(status) > 1:
            log.warning(f"More than one snapshot returned for {fsname}/{snapname}")
        else:
            log.debug(f"Snapshot status for {fsname}/{snapname}: {status}")
        if isinstance(status, dict):
            status = list(status.values())

        return status[0]

    # sleep_time will increase sleep time so we don't spam the logs
    def sleep_time(loopcount, progress):
        if loopcount > 12:
            if progress < 50:
                return 60.0   # if not progressing, sleep longer
            elif progress > 80:
                return 10.0
            else:
                return 30.0
        if loopcount > 9:
            if progress < 50:
                return 30.0   # if not progressing, sleep longer
            elif progress > 80:
                return 10.0
            else:
                return 20.0
        if loopcount > 6:
            if progress < 50:
                return 20.0   # if not progressing, sleep longer
            elif progress > 80:
                return 10.0
            else:
                return 15.0
        if loopcount > 3:
            if progress < 50:
                return 10.0   # if not progressing, sleep longer
            else:
                return 5.0    # first 25s
        return 2.0  # default

    def getFileSystems(cluster):
        try:
            fsdict = cluster.call_api(method="filesystems_list", parms={})
            return fsdict
        except Exception as exc:
            log.error(f"error getting filesystems: {exc}")
            return {}
    
    def getFilesystemBucketName(cluster, fsname, mode):
        fsdicts = getFileSystems(cluster)
        fsinfo = None
        buckets = []
        for fs in fsdicts:
            if fs['name'] == fsname:
                fsinfo = fs
                buckets = fsinfo['obs_buckets']
                break
        for b in buckets:
            if b['mode'].lower() == mode.lower():
                return b['name']
        return ''

    def getStatInfo(snap_stat, op):
        localstatus = snap_stat['localStowInfo']
        remotestatus = snap_stat['remoteStowInfo']
        if op == "upload":
            stowProgress = localstatus['stowProgress']
            stowStatus = localstatus['stowStatus']
            locator = localstatus['locator']
            obs_site = 'LOCAL'
            obs_mode = 'WRITABLE'
        else:
            stowProgress = remotestatus['stowProgress']
            stowStatus = remotestatus['stowStatus']
            locator = remotestatus['locator']
            obs_site = 'REMOTE'
            obs_mode = 'REMOTE'
        return stowProgress, stowStatus, locator, obs_site, obs_mode

    def upload_completed(fsname, snapname, op, uuid, locator='', bucketname='', reason="complete"):
        intent_log.put_record(uuid, fsname, snapname, op, "complete", loc=locator, bucket=bucketname)
        message = f"{op} complete: {fsname} - {snapname} locator: '{locator}' bucket: '{bucketname}'"
        if reason != "complete":
            message += f" ({reason})"
        background_q.message(message)
        actions_log.info(message)

    def upload_in_progress(fsname, snapname, op, uuid, locator='', bucketname=''):
        intent_log.put_record(uuid, fsname, snapname, op, "in-progress", loc=locator, bucket=bucketname)
        message = f"{op} started: {fsname} - {snapname} locator: '{locator}' bucket: '{bucketname}'"
        background_q.message(message)
        actions_log.info(message)

    def delete_completed(fsname, snapname, op, uuid, locator='', bucketname='', reason="deleted"):
        intent_log.put_record(uuid, fsname, snapname, "delete", "complete", loc=locator, bucket=bucketname)
        message = f"{op} complete: {fsname} - {snapname} locator: '{locator}' bucket: '{bucketname}'"
        if reason != "deleted":
            message += f" ({reason})"
        background_q.message(message)
        actions_log.info(message)

    def delete_in_progress(fsname, snapname, op, uuid, locator='', bucketname=''):
        intent_log.put_record(uuid, fsname, snapname, "delete", "complete", loc=locator, bucket=bucketname)
        message = f"{op} started: {fsname} - {snapname} locator: '{locator}' bucket: '{bucketname}'"
        background_q.message(message)
        actions_log.info(message)

    def upload_snap(q_upload_obj):
        # get the current snap status to make sure it looks valid
        fsname = q_upload_obj.fsname
        snapname = q_upload_obj.snapname
        op = q_upload_obj.operation
        uuid = q_upload_obj.uuid
        cluster = q_upload_obj.cluster
        locator = ''
        bq = background_q

        try:
            snap_stat = snapshot_status(q_upload_obj)
            log.info(f"snap_stat: {snap_stat}")
            # 'creationTime': '2021-05-14T15:15:00Z' - use to determine how long it takes to upload?
        except Exception as exc:
            log.error(f"unable to get snapshot status in upload {fsname}/{snapname}: {exc}")
            return

        if snap_stat == None:
            log.error(f"{fsname}/{snapname} doesn't exist.  Not created or already deleted?  Logging as complete...")
            upload_completed(fsname, snapname, op, uuid, reason="snapshot_missing")
            return

        stowProgress, stowStatus, locator, obs_site, obs_mode = getStatInfo(snap_stat, op)

        if stowStatus == "NONE":
            # Hasn't been uploaded yet; Try to upload the snap via API
            try:
                log.info(f"{op} snapshot {fsname}/{snapname} obs_site: {obs_site}")
                snaps = cluster.call_api(method="snapshot_upload",
                                            parms={'file_system': fsname, 
                                                    'snapshot': snapname,
                                                    'obs_site': obs_site})
                log.info(f"api result from upload call: {snaps}")
                locator = snaps['locator']
                bucketname = getFilesystemBucketName(cluster, fsname, obs_mode)
                log.info(f"{op} snapshot {fsname}/{snapname} obs_site: {obs_site} loc: '{locator}' bucketname: '{bucketname}'")
            except Exception as exc:
                log.error(f"error uploading snapshot {fsname}/{snapname}: {exc}")
                intent_log.put_record(uuid, fsname, snapname, op, "error")
                if "not tiered: cannot upload from it" in str(exc):     # mark complete if it can't upload
                    upload_completed(fsname, snapname, op, uuid, reason="filesystem_not_tiered")
                return  # skip the rest for this one

            log.info(f"Mark snapshot {op} as started for {fsname}/{snapname}")
            upload_in_progress(fsname, snapname, op, uuid, locator=locator, bucketname=bucketname)

        elif stowStatus == "SYNCHRONIZED":
            # we should only ever get here when replaying the log and this one was already in progress
            log.error(f"upload of {fsname}/{snapname} was already complete. Logging it as such")
            bucketname = getFilesystemBucketName(cluster, fsname, obs_mode)
            upload_completed(fsname, snapname, op, uuid, locator=locator, bucketname=bucketname)
            return

        # otherwise, it should be uploading, so we fall through and monitor it
        # monitor progress - we have to wait for this one to complete before uploading another
        sleeptime = 5.0
        loopcount = 0
        shortloopcount = 0
        while True:
            time.sleep(sleeptime)  # give it some time to upload, check in every 5s
            # get snap info via api
            try:
                shortloopcount += 1
                this_snap = snapshot_status(q_upload_obj)
            except Exception as exc:
                log.error(f"error listing snapshot status: checking status: {q_upload_obj} - {exc}")
                if shortloopcount > 10:   # Gotten errors 10 times for this upload, let it go
                    return
                else:
                    continue      # otherwise continue loop, try again
            # track how many times we're checking the status
            loopcount += 1
            if this_snap != None:
                stowProgress, stowStatus, locator, _, _ = getStatInfo(this_snap, op)

                if stowStatus == "UPLOADING":
                    progress = int(stowProgress[:-1])   # progress is something like "33%"
                    # reduce log spam - seems to hang under 50% for a while
                    sleeptime = sleep_time(loopcount, progress)
                    message = f"{op} of {fsname}/{snapname} in progress: {stowProgress} complete"
                    bq.message(message)
                    continue
                elif stowStatus == "SYNCHRONIZED":
                    upload_completed(fsname, snapname, op, uuid, locator=locator, bucketname=bucketname)
                    return
                elif stowStatus == "NONE" and stowProgress == 'N/A' and (op == "upload-remote" or op == "upload"):
                    log.info(f"{op} of {fsname}/{snapname} not started, waiting...")
                    time.sleep(5)
                    continue
                else:
                    message = f"{op} status of {fsname}/{snapname} is {stowStatus}/{stowProgress} - unexpected"
                    bq.message(message)
                    log.error(message)
                    return  # prevent infinite loop
            else:
                message = f"{op}: no snap status for {fsname}/{snapname}?"
                bq.message(message)
                return  
 
    def delete_snap(q_del_object):
        fsname = q_del_object.fsname
        snapname = q_del_object.snapname
        uuid = q_del_object.uuid
        cluster = q_del_object.cluster
        loc = q_del_object.loc
        bucket = q_del_object.bucket
        bucketname = ''
        message = f"Deleting snap {fsname}/{snapname}"
        bq = background_q
        bq.message(message)
        # maybe do a snap_status() so we know if it has an object locator and can reference the locator later?
        try:
            status = snapshot_status(q_del_object)
            if status:
                remote = status['remoteStowInfo']
                local = status['localStowInfo']
                log.info(f"snap_stat in delete_snap for {fsname}/{snapname}: site: {remote}/{local}")
        except Exception as exc:
            log.error(f"delete_snap: unable to get snapshot status for {fsname}/{snapname}: {exc}")
            return

        if status == None:
            # already gone? make sure it shows that way in the logs
            delete_completed(fsname, snapname, "delete", 
                uuid, locator=loc, bucketname=bucket, reason="not_found")
            return
        else:
            # locator = status['locator']
            locator = ''
            obs_mode = ''
            if locator == '':
                locator = status['remoteStowInfo']['locator']
                if locator != '':
                    obs_mode = 'REMOTE'
            if locator == '':
                locator = status['localStowInfo']['locator']
                if locator != '':
                    obs_mode = 'WRITABLE'
            if obs_mode != '':
                bucketname = getFilesystemBucketName(cluster, fsname, obs_mode)
        try:
            # ask cluster to delete the snap
            result = cluster.call_api(method="snapshot_delete",
                                        parms={"file_system": fsname, "name": snapname})
            log.info(f"Delete result from {fsname}/{snapname}: {result}")
            log.info(f"Snap {fsname}/{snapname} delete initiated")
        except Exception as exc:
            log.error(f"Error deleting snap {fsname}/{snapname} : {exc} - skipping for now")
            return

        delete_in_progress(fsname, snapname, "delete", uuid, locator=locator, bucketname=bucketname)

        # delete may take some time, particularly if uploaded to obj and it's big
        time.sleep(1)  # give just a little time, just in case it's instant
        loopcount = 0
        while True:
            # if may happen quickly, so sleep at the end of the cycle
            try:
                this_snap = snapshot_status(q_del_object)
            except Exception as exc:
                # when the snap no longer exists, we get a None back, so this == an error
                # log.debug(f"snap delete raised exception")
                log.error(f"Error getting snapshot status: {exc}")
                return

            # when the snap no longer exists, we get a None from snap_status()
            if this_snap == None:
                delete_completed(fsname, snapname, "delete", uuid, locator=locator, bucketname=bucketname)
                return
            # track how many times we're checking the status
            loopcount += 1
            if this_snap['objectProgress'] == 'N/A' and this_snap['stowStatus'] == "NONE":   # wasn't uploaded.
                log.debug(f"delete_snap: snap {fsname}/{snapname} wasn't uploaded (stowStatus NONE)")
                progress = -1
            elif '%' in this_snap['objectProgress']:
                progress = int(this_snap['objectProgress'][:-1])  # progress is something like "33%", remove last char
            else:
                progress = 0
            message = f"   Delete of {fsname}/{snapname} progress: {this_snap['objectProgress']}"
            bq.message(message)

            # reduce log spam - seems to hang under 50% for a while (only if it was uploaded)
            sleeptime = sleep_time(loopcount, progress)
            time.sleep(sleeptime)  # give it some time to delete, check in based on progress/loop count

    #
    # main background_processor() logic here:
    #

    main_thread = threading.main_thread()

    time.sleep(10)  # delay start until something happens.  ;)
    log.info("background_uploader starting...")

    while True:
        # take item off queue
        try:
            # don't block forever so we can keep an eye on the main thread
            # background_q.get() returns a QueueOperation object
            snapq_op = background_q.get(block=True, timeout=1)  # block for up to 1s
        except queue.Empty:
            # log.debug(f"Queue get timed out; nothing in queue.")
            if main_thread.is_alive():
                # log.debug(f"Main thread is alive")
                continue
            else:
                log.debug(f"Main thread is dead, exiting uploader thread")
                # main thread died - exit so the program exits; we can't live without the main thread
                return

        log.debug(f"Queue entry received {snapq_op.fsname}, {snapq_op.snapname}, {snapq_op.operation}")

        if snapq_op.fsname == "WEKA_TERMINATE_THREAD" and snapq_op.snapname == "WEKA_TERMINATE_THREAD":
            log.info(f"background_processor: terminating thread")
            return

        if snapq_op.operation == "upload" or snapq_op.operation == "upload-remote":
            time.sleep(3)   # slow down... make sure the snap is settled.
            upload_snap(snapq_op)   # handles its own errors
        elif snapq_op.operation == "delete":
            time.sleep(0.3)   # less time between deletes
            delete_snap(snapq_op)
        # elif snap.operation == "create":
        #     create_snap(snap)


# module init
def init_background_q():
    global intent_log
    if intent_log == 'Global uninitialized':
        intent_log = IntentLog(intent_log_filename)
        # background_q.locators = intent_log.undeleted_locators()
        background_q.locators = intent_log.get_records_pd()
        # start the upload thread
        background_q_thread = threading.Thread(target=background_processor)
        background_q_thread.daemon = True
        background_q_thread.start()
        log.info(f"background_thread = {background_q_thread}")
        background_q.message("Upload/download queue process started...")
    return intent_log


if __name__ == "__main__":

    intent_log = init_background_q()

    time.sleep(2)

    intent_log.put_record('uuid1', "fs1", 'snap1', "upload", "queued")
    intent_log.put_record('uuid2', "fs1", 'snap2', "upload", "queued")
    intent_log.put_record('uuid3', "fs1", 'snap3', "delete", "queued")
    intent_log.put_record('uuid4', "fs1", 'snap4', "delete", "queued")

    intent_log.put_record('uuid1', "fs1", 'snap1', "upload", "in-progress")
    intent_log.put_record('uuid2', "fs1", 'snap2', "upload", "in-progress")

    intent_log.put_record('uuid1', "fs1", 'snap1', "upload-remote", "complete")
"""
    for uuid, fs, operation, snaprec in intent_log._incomplete_records():
        print(f"uuid={uuid}, fs={fs}, operation'{operation}, snap={snaprec}")


    logging.debug(f"first test")
    UploadSnapshot(background_q, "fs1", "snap1") # should be cluster instead of background_q

    logging.debug(f"second test")
    UploadSnapshot(background_q, "fs2", "snap2")


    logging.debug(f"third test")
    UploadSnapshot(background_q, "fs3", "snap3")

    logging.debug(f"fourth test")
    UploadSnapshot(background_q, "fs4", "snap4")


    logging.debug(f"fifth test")
    UploadSnapshot(background_q, "fs5", "snap5")

    logging.debug(f"sixth test")
    UploadSnapshot(background_q, "fs6", "snap6")

    logging.debug(f"terminating")
    UploadSnapshot(background_q, "WEKA_TERMINATE_THREAD", "WEKA_TERMINATE_THREAD")

    time.sleep(15)

    background_q_thread.join()
"""
