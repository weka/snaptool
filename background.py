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

log = logging.getLogger(__name__)
snaplog = logging.getLogger("snapshot")

class IntentLog:
    def __init__(self, logfilename):
        self._lock = threading.Lock()
        self.filename = logfilename

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
    def put_record(self, uuid_s, fsname, snapname, snapop, status):
        with self._lock:
            with open(self.filename, "a") as fd:
                fd.write(f"{uuid_s}:{fsname}:{snapname}:{snapop}:{status}\n")

    # replay the log on a cluster
    def replay(self, cluster):
        log.info(f"Replaying upload intent log")
        replay_start = time.time()
        for uuid_str, fsname, snapname, snapop in self._incomplete_records():
            log.info(f"re-scheduling {fsname}/{snapname}")
            QueueOperation(cluster, fsname, snapname, snapop, uuid_str=uuid_str)
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
        for uid, fsname, snapname, clusterop, status in self._records():
            if uid not in snaps:
                snapshot = dict()
                snapshot['fsname'] = fsname
                snapshot['snapname'] = snapname
                snapshot['operation'] = clusterop
                snapshot['status'] = status
                snaps[uid] = snapshot
            else:
                if status == "complete":
                    log.debug(f"De-queuing snap {uid} {fsname}/{snapname} (complete)")
                    del snaps[uid]  # remove ones that completed so we don't need to look through them
                else:
                    log.debug(f"Updating status of snap {uid} {fsname}/{snapname} to {status}")
                    snaps[uid]['status'] = status  # update status

        # this should be a very short list - far less than 100; likely under 5
        log.info(f"intent-log incomplete records len: {len(snaps)}; snaps = {snaps}")
        sorted_snaps = {"queued": {}, "in-progress": {}, "error": {}}

        for uid, snapshot in snaps.items():  # sort so we can see in-progress and error first
            log.debug(f"uuid={uid}, snapshot={snapshot}")
            sorted_snaps[snapshot['status']][uid] = snapshot

        log.debug(f"sorted_snaps = {sorted_snaps}")
        log.debug(
            f"There are {len(sorted_snaps['error'])} error snaps, {len(sorted_snaps['in-progress'])} in-progress"
            f" snaps, and {len(sorted_snaps['queued'])} queued snaps in the intent log")

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


"""
# not used - here for reverse testing base_n_to_int

def base_62_to_int(base_62_num: str):
    result = 0
    base = len(base_62_digits)
    num_digits = len(base_62_num)
    for i, c in enumerate(base_62_num):
        result += base_62_digits.index(c)
        if i < num_digits - 1:
            result *= base
    return result
"""

def get_short_unique_id():     # returns a uuid4 that has been converted to base 62
    number = uuid.uuid4().int
    result = int_to_base_62(number)
    if len(result) < 21:        # prefer a consistent length and most are 22 chars long, fill to 22
        result.zfill(22)
    return result

class QueueOperation:
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

        if fsname != "WEKA_TERMINATE_THREAD" and snapname != "WEKA_TERMINATE_THREAD":
            intent_log.put_record(self.uuid, fsname, snapname, op, "queued")
        # queue the request
        background_q.put(self)

# process operations in the background - runs in a thread - starts before replaying log
def background_processor():
    global background_q      # queue of QueueOperation objects
    global intent_log       # log file(s) for all QueueOperation objects created, for replay if necessary

    def snapshot_status(snapobj):
        # get snap info via api - assumes snap has been created already
        try:
            status = snapobj.cluster.call_api(method="snapshots_list",
                                              parms={'file_system': snapobj.fsname, 'name': snapobj.snapname})
        except Exception as exc:
            log.error(f"Error getting snapshot status for {snapobj.fsname}/{snapobj.snapname}: {exc}")
            raise  # API error - let calling routine handle it

        if len(status) == 0:
            # hmm... this one doesn't exist on the cluster? Let calling routine handle it
            # might be on purpose, or checking that it got deleted
            return None
        elif len(status) > 1:
            log.warning(f"More than one snapshot returned for {snapobj.fsname}/{snapobj.snapname}")
        else:
            log.debug(f"Snapshot status for {snapobj.fsname}/{snapobj.snapname}: {status}")

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
        return 5.0  # default

    def upload_snap(snapobj):
        # get the current snap status to make sure it looks valid
        try:
            snap_stat = snapshot_status(snapobj)
            # 'creationTime': '2021-05-14T15:15:00Z' - use to determine how long it takes to upload?
        except Exception as exc:
            log.error(f"unable to upload snapshot {snapobj.fsname}/{snapobj.snapname}: {exc}")
            return

        if snap_stat is None:
            log.error(f"{snapobj.fsname}/{snapobj.snapname} doesn't exist! Did creation fail?  Logging as complete...")
            intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "upload", "complete")
            return

        if snap_stat["stowStatus"] == "NONE":
            # Hasn't been uploaded yet; Try to upload the snap via API
            try:
                snaps = snapobj.cluster.call_api(method="snapshot_upload",
                                                 parms={'file_system': snapobj.fsname, 'snapshot': snapobj.snapname})
                # snapshots = {'extra': None, 'locator': '2561d133/d/s/28/spec/6ff5-4523-adfe-9255e506de76'}
            except Exception as exc:
                log.error(f"error uploading snapshot {snapobj.fsname}/{snapobj.snapname}: {exc}")
                intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "upload", "error")
                return  # skip the rest for this one

            # log that it's been told to upload
            # log.debug(f"snapshots = {snapshots}") # ***vince - check the return to make sure it's been told to upload

            log.info(f"uploading snapshot {snapobj.fsname}/{snapobj.snapname}")
            intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "upload", "in-progress")
            snaplog.info(f"Upload initiated:{snapobj.fsname}:{snapobj.snapname}:{snaps['locator']}")

        elif snap_stat["stowStatus"] == "SYNCHRONIZED":
            # we should only ever get here when replaying the log and this one was already in progress
            log.error(f"upload of {snapobj.fsname}/{snapobj.snapname} was already complete. Logging it as such")
            intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "upload", "complete")
            return

        # otherwise, it should be uploading, so we fall through and monitor it
        # monitor progress - we have to wait for this one to complete before uploading another
        sleeptime = 5.0
        loopcount = 0
        while True:
            time.sleep(sleeptime)  # give it some time to upload, check in every 5s
            # get snap info via api
            try:
                this_snap = snapshot_status(snapobj)
            except Exception as exc:
                log.error(f"error listing snapshots: checking status: {exc}")
                return

            # track how many times we're checking the status
            loopcount += 1

            if this_snap is not None:
                if this_snap["stowStatus"] == "UPLOADING":
                    progress = int(this_snap['objectProgress'][:-1])   # progress is something like "33%"
                    # reduce log spam - seems to hang under 50% for a while
                    sleeptime = sleep_time(loopcount, progress)
                    log.info(
                        f"upload of {snapobj.fsname}/{snapobj.snapname} in progress: "
                        f"{this_snap['objectProgress']} complete")
                    continue
                elif this_snap["stowStatus"] == "SYNCHRONIZED":
                    log.info(f"upload of {snapobj.fsname}/{snapobj.snapname} complete.")
                    intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "upload", "complete")
                    snaplog.info(f"Upload complete:{snapobj.fsname}:{snapobj.snapname}:{this_snap['locator']}")
                    return
                else:
                    log.error(
                        f"upload status of {snapobj.fsname}/{snapobj.snapname} is {this_snap['stowStatus']}/" +
                        f"{this_snap['objectProgress']}?")
                    return  # prevent infinite loop
            else:
                log.error(f"no snap status for {snapobj.fsname}/{snapobj.snapname}?")
                return

    def delete_snap(snapobj):
        log.debug(f"deleting snap {snapobj.fsname}/{snapobj.snapname}")
        # maybe do a snap_status() so we know if it has an object locator and can reference the locator later?
        try:
            status = snapshot_status(snapobj)
        except Exception as exc:
            log.error(f"delete_snap: unable to get snapshot status for {snapobj.fsname}/{snapobj.snapname}: {exc}")
            return

        if status is None:
            # already gone? make sure it shows that way in the logs
            intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "delete", "complete")
            log.info(f"snap {snapobj.fsname}/{snapobj.snapname} was deleted already")
            return
        else:
            locator = status['locator']

        try:
            # ask cluster to delete the snap
            result = snapobj.cluster.call_api(method="snapshot_delete",
                                              parms={"file_system": snapobj.fsname, "name": snapobj.snapname})
            log.debug(f"delete result: {result}")
            log.info(f"snap {snapobj.fsname}/{snapobj.snapname} delete initiated")
        except Exception as exc:
            log.error(f"error deleting snapshot {snapobj.snapname} from filesystem {snapobj.fsname}: {exc}")

        intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "delete", "in-progress")
        snaplog.info(f"Delete Initiated:{snapobj.fsname}/{snapobj.snapname}:{locator}")

        # delete may take some time, particularly if uploaded to obj and it's big
        time.sleep(1)  # give just a little time, just in case it's instant
        loopcount = 0
        while True:
            # if may happen quickly, so sleep at the end of the cycle
            try:
                this_snap = snapshot_status(snapobj)
            except Exception as exc:
                # when the snap no longer exists, we get a None back, so this is an error
                # log.debug(f"snap delete raised exception")
                log.error(f"error listing snapshots: checking status: {exc}")
                return

            # when the snap no longer exists, we get a None from snap_status()
            if this_snap is None:
                intent_log.put_record(snapobj.uuid, snapobj.fsname, snapobj.snapname, "delete", "complete")
                log.info(f"snap {snapobj.fsname}/{snapobj.snapname} sucessfully deleted")
                snaplog.info(f"Delete complete:{snapobj.fsname}:{snapobj.snapname}:{locator}")
                return
            # track how many times we're checking the status
            loopcount += 1
            if this_snap['objectProgress'] == 'N/A' and this_snap['stowStatus'] == "NONE":   # wasn't uploaded.
                log.info(f"delete_snap: snap {snapobj.fsname}/{snapobj.snapname} wasn't uploaded (stowStatus NONE)")
                progress = -1
            else:
                progress = int(this_snap['objectProgress'][:-1])  # progress is something like "33%", remove last char
            log.info(f"delete of {snapobj.fsname}/{snapobj.snapname} progress: {this_snap['objectProgress']}")

            # reduce log spam - seems to hang under 50% for a while (only if it was uploaded)
            if progress >= 0:
                sleeptime = sleep_time(loopcount, progress)
            else:
                sleeptime = 5

            time.sleep(sleeptime)  # give it some time to delete, check in every 5s or possibly longer if was uploaded

    """
    # not using this yet (maybe never)... accesspoint_name is a little issue
    def create_snap(snap):
        log.debug(f"creating snap {snap.fsname}/{snap.snapname}")

        log.debug(f"snap {snap.snapname} to be created on fs {snap.fsname}")
        try:
            created_snap = snap.cluster.call_api(method="snapshot_create", parms={
                "file_system": snap.fsname,
                "name": snap.snapname,
                "access_point": snap.accesspoint_name,
                "is_writable": False})
            log.info(f"snap {snap.snapname} has been created on fs {snap.fsname}")
            # needs error-checking
        except Exception as exc:
            log.error(f"error creating snapshot {snap.snapname} on filesystem {snap.fsname}: {exc}")
    """

    #
    # main background_processor() logic here:
    #

    main_thread = threading.main_thread()

    time.sleep(30)  # delay start until something happens.  ;)
    log.info("background_uploader starting...")

    while True:
        # take item off queue
        try:
            # don't block forever so we can keep an eye on the main thread
            # background_q.get() returns a QueueOperation object
            # log.debug(f"Getting from queue")
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

        time.sleep(5)   # slow down... make sure the snap is settled.

        if snapq_op.operation == "upload":
            upload_snap(snapq_op)   # handles it's own errors
        elif snapq_op.operation == "delete":
            delete_snap(snapq_op)
        # elif snap.operation == "create":
        #     create_snap(snap)


# module init
# upload queue for queuing object uploads
background_q = queue.Queue()
# intent log
intent_log = IntentLog("upload_intent.log")

# start the upload thread
upload_thread = threading.Thread(target=background_processor)
upload_thread.daemon = True
upload_thread.start()
log.info(f"upload_thread = {upload_thread}")

if __name__ == "__main__":

    time.sleep(2)

    intent_log.put_record('uuid1', "fs1", 'snap1', "upload", "queued")
    intent_log.put_record('uuid2', "fs1", 'snap2', "upload", "queued")
    intent_log.put_record('uuid3', "fs1", 'snap3', "delete", "queued")
    intent_log.put_record('uuid4', "fs1", 'snap4', "delete", "queued")

    intent_log.put_record('uuid1', "fs1", 'snap1', "upload", "in-progress")
    intent_log.put_record('uuid2', "fs1", 'snap2', "upload", "in-progress")

    intent_log.put_record('uuid1', "fs1", 'snap1', "upload", "complete")
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

    upload_thread.join()
"""
