#!/usr/bin/env python3

# background.py - manage snapshot uploads
# Vince Fleming
# vince@weka.io
#
# cleanup for new scheduling and getting rid of warnings - Bruce Clagett bruce@weka.io

# system imports
import os
import queue
import threading
import time
import uuid
import logging
import string

logdir = "logs"
log = logging.getLogger(__name__)
actions_log = logging.getLogger("snapshot_actions_log")
intent_log_filename = "snap_intent_q.log"
intent_log = 'Global uninitialized'
background_q = queue.Queue()

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
    def put_record(self, uuid_s, fsname, snapname, snap_op, status):
        with self._lock:
            with open(self.filename, "a") as fd:
                fd.write(f"{uuid_s}:{fsname}:{snapname}:{snap_op}:{status}\n")

    # replay the log on a cluster
    def replay(self, cluster):
        log.info(f"Replaying background intent log")
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
                            temp = record.split(':')
                            yield temp[0], temp[1], temp[2], temp[3], temp[4][:-1]
                except FileNotFoundError:
                    log.info(f"Log file {filename} not found")
                    continue

    # un-completed records - an iterable
    def _incomplete_records(self):
        snaps = {}
        # distill the records to just ones that need to be re-processed
        log.info("Reading intent log")
        for uid, fsname, snapname, cluster_op, status in self._records():
            if uid not in snaps:
                snapshot = dict()
                snapshot['fsname'] = fsname
                snapshot['snapname'] = snapname
                snapshot['operation'] = cluster_op
                snapshot['status'] = status
                snapshot['uid'] = uid
                snaps[uid] = snapshot
            else:
                if status == "complete":
                    log.debug(f"De-queuing snap {uid} {fsname}/{snapname} (complete)")
                    del snaps[uid]  # remove ones that completed so we don't need to look through them
                else:
                    log.debug(f"Updating status of snap {uid} {fsname}/{snapname} to {status}")
                    snaps[uid]['status'] = status  # update status

        # this should be a very short list - far less than 100; likely under 5
        sorted_snaps = {"queued": {}, "in-progress": {}, "error": {}, "complete": {}}

        for uid, snapshot in snaps.items():  # sort so we can see in-progress and error first
            log.debug(f"uuid={uid}, snapshot={snapshot}")
            sorted_snaps[snapshot['status']][uid] = snapshot

        for uid, s in sorted_snaps['complete'].items():
            if s['operation'] == "delete":
                del snaps[uid]

        log.debug(f"sorted_snaps = {sorted_snaps}")
        log.debug(
            f"There are {len(sorted_snaps['error'])} error snaps, {len(sorted_snaps['in-progress'])} in-progress"
            f" snaps, and {len(sorted_snaps['queued'])} queued snaps in the intent log")

        log.info(f"intent-log incomplete records len: {len(snaps)}; snaps = {snaps}")

        # process in order of status # not sure about error ones... do we re-queue?  Should only be 1 in-progress too
        for status in ["in-progress", "error", "queued"]:
            for uid, snapshot in sorted_snaps[status].items():
                # these should be re-queued because they didn't finish
                log.debug(f"re-queueing snapshot = {snapshot}, status={status}")
                yield uid, snapshot['fsname'], snapshot['snapname'], snapshot['operation']


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
    def __init__(self, cluster, fsname, snapname, op, uuid_str=None):
        global background_q
        global intent_log

        self.fsname = fsname
        self.snapname = snapname
        self.operation = op
        self.cluster = cluster

        if uuid_str is None:
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

# process operations in the background - runs in a thread - starts before replaying log
def background_processor():
    global background_q      # queue of QueueOperation objects
    global intent_log       # log file(s) for all QueueOperation objects created, for replay if necessary

    def snapshot_status(q_snap_obj):
        fsname = q_snap_obj.fsname
        snapname = q_snap_obj.snapname
        # get snap info via api - assumes snap has been created already
        status = []
        for i in range(3):   # try 3 times on some errors
            try:
                status = q_snap_obj.cluster.call_api(method="snapshots_list",
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

    def getStatInfo(snap_stat, op):
        localstatus = snap_stat['localStowInfo']
        remotestatus = snap_stat['remoteStowInfo']
        if op is "upload":
            stowProgress = localstatus['stowProgress']
            stowStatus = localstatus['stowStatus']
        else:
            stowProgress = remotestatus['stowProgress']
            stowStatus = remotestatus['stowStatus']
        return stowProgress, stowStatus

    def upload_snap(q_upload_obj):
        # get the current snap status to make sure it looks valid
        fsname = q_upload_obj.fsname
        snapname = q_upload_obj.snapname
        op = q_upload_obj.operation
        uuid = q_upload_obj.uuid
        locator = None

        try:
            snap_stat = snapshot_status(q_upload_obj)
            log.debug(f"snap_stat: {snap_stat}")
            # 'creationTime': '2021-05-14T15:15:00Z' - use to determine how long it takes to upload?
        except Exception as exc:
            log.error(f"unable to get snapshot status in upload {fsname}/{snapname}: {exc}")
            return

        if snap_stat is None:
            log.error(f"{fsname}/{snapname} doesn't exist.  Not created?  Logging as complete...")
            intent_log.put_record(uuid, fsname, snapname, op, "complete")
            return

        stowProgress, stowStatus = getStatInfo(snap_stat, op)

        if stowStatus == "NONE":
            # Hasn't been uploaded yet; Try to upload the snap via API
            try:
                log.info(f"Calling snapshot upload with obs_site: {op}")
                log.info(f"     upload operation spec: {op}")
                if op == "upload-remote":
                    obs_site = 'REMOTE'
                else:
                    obs_site = 'LOCAL'
                snaps = q_upload_obj.cluster.call_api(method="snapshot_upload",
                                                  parms={'file_system': fsname, 
                                                         'snapshot': snapname,
                                                         'obs_site': obs_site})
                locator = snaps['locator']
            except Exception as exc:
                log.error(f"error uploading snapshot {fsname}/{snapname}: {exc}")
                intent_log.put_record(uuid, fsname, snapname, op, "error")
                if "not tiered: cannot upload from it" in str(exc):     # mark complete if it can't upload
                    intent_log.put_record(uuid, fsname, snapname, op, "complete")
                return  # skip the rest for this one

            # log that it's been told to upload
            # log.debug(f"snapshots = {snapshots}") # ***vince - check the return to make sure it's been told to upload

            log.info(f"Queueing snapshot {op} for {fsname}/{snapname}")
            intent_log.put_record(uuid, fsname, snapname, op, "in-progress")
            actions_log.info(f"{op} initiated: {fsname} - {snapname} locator: '{locator}'")

        elif stowStatus == "SYNCHRONIZED":
            # we should only ever get here when replaying the log and this one was already in progress
            log.error(f"upload of {fsname}/{snapname} was already complete. Logging it as such")
            intent_log.put_record(uuid, fsname, snapname, op, "complete")
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
            if this_snap is not None:
                stowProgress, stowStatus = getStatInfo(this_snap, op)

                if stowStatus == "UPLOADING":
                    progress = int(stowProgress[:-1])   # progress is something like "33%"
                    # reduce log spam - seems to hang under 50% for a while
                    sleeptime = sleep_time(loopcount, progress)
                    log.info(
                        f"upload of {fsname}/{snapname} in progress: {stowProgress} complete")
                    continue
                elif stowStatus == "SYNCHRONIZED":
                    log.info(f"upload of {fsname}/{snapname} complete.")
                    intent_log.put_record(uuid, fsname, snapname, op, "complete")
                    actions_log.info(f"{op} complete: {fsname} - {snapname} locator: '{locator}'")
                    return
                elif stowStatus == "NONE" and stowProgress == 'N/A' and op == "upload-remote":
                    log.info(f"{op} of {fsname}/{snapname} not started, waiting...")
                    time.sleep(5)
                    continue
                else:
                    log.error(
                        f"upload status of {fsname}/{snapname} is {stowStatus}/{stowProgress}?")
                    return  # prevent infinite loop
            else:
                log.error(f"no snap status for {fsname}/{snapname}?")
                return

    def delete_snap(q_del_object):
        fsname = q_del_object.fsname
        snapname = q_del_object.snapname
        uuid = q_del_object.uuid
        log.info(f"Deleting snap {fsname}/{snapname}")
        # maybe do a snap_status() so we know if it has an object locator and can reference the locator later?
        try:
            status = snapshot_status(q_del_object)
            log.debug(f"snap_stat: {status}")
        except Exception as exc:
            log.error(f"delete_snap: unable to get snapshot status for {fsname}/{snapname}: {exc}")
            return

        if status is None:
            # already gone? make sure it shows that way in the logs
            intent_log.put_record(uuid, fsname, snapname, "delete", "complete")
            log.info(f"Snap {fsname}/{snapname} was deleted already; marked complete in intent log")
            return
        else:
            locator = status['locator']
            if locator is '':
                locator = status['remoteStowInfo']['locator']
            if locator is '':
                locator = status['localStowInfo']['locator']

        try:
            # ask cluster to delete the snap
            result = q_del_object.cluster.call_api(method="snapshot_delete",
                                               parms={"file_system": fsname, "name": snapname})
            log.debug(f"Delete result: {result}")
            log.debug(f"Snap {fsname}/{snapname} delete initiated")
        except Exception as exc:
            log.error(f"Error deleting snap {fsname}/{snapname} : {exc} - skipping for now")
            return

        intent_log.put_record(uuid, fsname, snapname, "delete", "in-progress")
        actions_log.info(f"delete started: {fsname} - {snapname} locator: '{locator}'")

        # delete may take some time, particularly if uploaded to obj and it's big
        time.sleep(1)  # give just a little time, just in case it's instant
        loopcount = 0
        while True:
            # if may happen quickly, so sleep at the end of the cycle
            try:
                this_snap = snapshot_status(q_del_object)
            except Exception as exc:
                # when the snap no longer exists, we get a None back, so this is an error
                # log.debug(f"snap delete raised exception")
                log.error(f"Error getting snapshot status: {exc}")
                return

            # when the snap no longer exists, we get a None from snap_status()
            if this_snap is None:
                intent_log.put_record(uuid, fsname, snapname, "delete", "complete")
                log.info(f"     Snap {fsname}/{snapname} successfully deleted")
                actions_log.info(f"delete complete: {fsname} - {snapname} locator: '{locator}'")
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
            log.info(f"   Delete of {fsname}/{snapname} progress: {this_snap['objectProgress']}")

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
    # intent log
    if intent_log == 'Global uninitialized':
        intent_log = IntentLog(intent_log_filename)

        # start the upload thread
        background_q_thread = threading.Thread(target=background_processor)
        background_q_thread.daemon = True
        background_q_thread.start()
        log.info(f"background_thread = {background_q_thread}")
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
