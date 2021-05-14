#!/usr/bin/env python3

# background.py - manage snapshot uploads
# Vince Fleming
# vince@weka.io
#

# system imports
import os
# import datetime
# from urllib3 import add_stderr_logger
import queue
import threading
import time
import uuid
from logging import getLogger
from threading import Lock

# from wekacluster import WekaCluster
# import signals
# from snapshots import SnapSchedule, MonthlySchedule, WeeklySchedule, DailySchedule, HourlySchedule

log = getLogger(__name__)
snaplog = getLogger("snapshot")

class IntentLog:
    def __init__(self, logfilename):
        self._lock = Lock()
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
    def put_record(self, uuid, fsname, snapname, operation, status):
        with self._lock:
            with open(self.filename, "a") as fd:
                fd.write(f"{uuid}:{fsname}:{snapname}:{operation}:{status}\n")

    # replay the log on a cluster
    def replay(self, cluster):
        log.info(f"Replaying upload intent log")
        for uuid, fsname, snapname, operation in self._incomplete_records():
            log.info(f"re-scheduling {fsname}/{snapname}")
            QueueOperation(cluster, fsname, snapname, operation, uuid=uuid)

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
        for uuid, fsname, snapname, operation, status in intent_log._records():
            if uuid not in snaps:
                snapshot = dict()
                snapshot['fsname'] = fsname
                snapshot['snapname'] = snapname
                snapshot['operation'] = operation
                snapshot['status'] = status
                snaps[uuid] = snapshot

                #snaps[uuid] = {}
                #snaps[uuid]['fsname'] = fsname
                #snaps[uuid]['snapname'] = snapname
                #snaps[uuid]['operation'] = operation
                #snaps[uuid]['status'] = status
            else:
                # log.debug(f"status is '{status}'")
                if status == "complete":
                    log.debug(f"Dequeueing complete snap {uuid}")
                    del snaps[uuid]  # remove ones that completed so we don't need to look through them
                else:
                    log.debug(f"Updating status of snap {uuid} to {status}")
                    snaps[uuid]['status'] = status  # update status

        log.debug(f"snaps = {snaps}")  # this should be a very short list - far less than 100; likely under 5

        sorted_snaps = {"queued": {}, "in-progress": {}, "error": {}}

        for uuid, snapshot in snaps.items():  # sort so we can see in-progress and error first
            log.debug(f"uuid={uuid}, snapshot={snapshot}")
            sorted_snaps[snapshot['status']][uuid] = snapshot

        log.debug(f"sorted_snaps = {sorted_snaps}")
        log.debug(
            f"There are {len(sorted_snaps['error'])} error snaps, {len(sorted_snaps['in-progress'])} in-progress" +
            f" snaps, and {len(sorted_snaps['queued'])} queued snaps in the intent log")

        # process in order of status # not sure about error ones... do we re-queue?  Should only be 1 in-progress too
        for status in ["in-progress", "error", "queued"]:
            for uuid, snapshot in sorted_snaps[status].items():
                # these should be re-queued because they didn't finish
                log.debug(f"re-queueing snapshot = {snapshot}, status={status}")
                yield uuid, snapshot['fsname'], snapshot['snapname'], snapshot['operation']


def unique_id(alphabet='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    number = uuid.uuid4().int
    result = ''
    while number != 0:
        number, i = divmod(number, len(alphabet))
        result = alphabet[i] + result
    return result


#class UploadSnapshot:
class QueueOperation:
    def __init__(self, cluster, fsname, snapname, operation, uuid=None):
        global uploadq
        global intent_log
        self.fsname = fsname
        self.snapname = snapname
        self.operation = operation
        self.cluster_obj = cluster

        if uuid is None:
            self.uuid = unique_id()
        else:
            self.uuid = uuid
        if fsname != "WEKA_TERMINATE_THREAD" and snapname != "WEKA_TERMINATE_THREAD":
            intent_log.put_record(self.uuid, fsname, snapname, operation, "queued")
        # queue the request
        uploadq.put(self)


# starts the background uploader
# def start_uploader():
#    upload_thread = threading.Thread(target=background_uploader)
#    upload_thread.start()
#    log.info(f"upload_thread = {upload_thread}")


# process operations in the background - runs in a thread - starts before replaying log
def background_processor():
    global uploadq
    global intent_log

    def snapshot_status(snap):
        # get snap info via api - assumes snap has been created already
        try:
            status = snap.cluster_obj.call_api(method="snapshots_list",
                                          parms={'file_system': snap.fsname, 'name': snap.snapname})
        except Exception as exc:
            log.error(f"error listing snapshots: checking status: {exc}")
            raise  # API error - let calling routine handle it

        if len(status) == 0:
            # hmm... this one doesn't exist on the cluster? Let calling routine handle it
            return None
        else:
            #log.debug(f"snap_stat is {status}")
            pass

        return status[0]

    # sleeptimer will increase sleep time so we don't spam the logs
    def sleeptimer(loopcount, progress):
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


    def upload_snap(snap):
        # get the current snap status to make sure it looks valid
        try:
            snap_stat = snapshot_status(snap)
            # 'creationTime': '2021-05-14T15:15:00Z' - use to determine how long it takes to upload?
        except Exception:
            log.error(f"unable to upload snapshot {snap.fsname}/{snap.snapname}")
            return

        if snap_stat == None:
            log.error(f"{snap.fsname}/{snap.snapname} doesn't exist! Did creation fail?  Logging as complete...")
            intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "upload", "complete")
            return

        if snap_stat["stowStatus"] == "NONE":
            # Hasn't been uploaded yet; Try to upload the snap via API
            try:
                snapshots = snap.cluster_obj.call_api(method="snapshot_upload",
                                                 parms={'file_system': snap.fsname, 'snapshot': snap.snapname})
                # snapshots = {'extra': None, 'locator': '2561d133/d/s/28/spec/6ff5-4523-adfe-9255e506de76'}
            except Exception as exc:
                log.error(f"error uploading snapshot {snap.fsname}/{snap.snapname}: {exc}")
                intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "upload", "error")
                return  # skip the rest for this one

            # log that it's been told to upload
           # log.debug(f"snapshots = {snapshots}")   # ***vince - check the return to make sure it's been told to upload

            log.info(f"uploading snapshot {snap.fsname}/{snap.snapname}")
            intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "upload", "in-progress")
            snaplog.info(f"Upload initiated:{snap.fsname}:{snap.snapname}:{snapshots['locator']}")

        elif snap_stat["stowStatus"] == "SYNCHRONIZED":
            # we should only ever get here when replaying the log and this one was already in progress
            log.error(f"upload of {snap.fsname}/{snap.snapname} was already complete. Logging it as such")
            intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "upload", "complete")
            return

        # otherwise, it should be uploading, so we fall through and monitor it

        # monitor progress - we have to wait for this one to complete before uploading another
        sleeptime = 5.0
        loopcount = 0
        while True:
            time.sleep(sleeptime)  # give it some time to upload, check in every 5s
            # get snap info via api
            try:
                this_snap = snapshot_status(snap)
            except Exception:
                log.error(f"error listing snapshots: checking status")
                return

            # track how many times we're checking the status
            loopcount += 1

            if this_snap is not None:
                if this_snap["stowStatus"] == "UPLOADING":
                    progress = int(this_snap['objectProgress'][:-1])   # progress is something like "33%"
                    # reduce log spam - seems to hang under 50% for a while
                    sleeptime = sleeptimer(loopcount, progress)
                    log.info(
                        f"upload of {snap.fsname}/{snap.snapname} in progress: {this_snap['objectProgress']} complete")
                    continue
                elif this_snap["stowStatus"] == "SYNCHRONIZED":
                    log.info(f"upload of {snap.fsname}/{snap.snapname} complete.")
                    intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "upload", "complete")
                    snaplog.info(f"Upload complete:{snap.fsname}:{snap.snapname}:{this_snap['locator']}")
                    return
                else:
                    log.error(
                        f"upload status of {snap.fsname}/{snap.snapname} is {this_snap['stowStatus']}/" +
                        f"{this_snap['objectProgress']}?")
                    return  # prevent infinite loop
            else:
                log.error(f"no snap status for {snap.fsname}/{snap.snapname}?")
                return

    def delete_snap(snap):
        log.debug(f"deleting snap {snap.fsname}/{snap.snapname}")
        # maybe do a snap_status() so we know if it has an object locator and can reference the locator later?
        try:
            status = snapshot_status(snap)
        except:
            log.error(f"unable to delete snapshot {snap.fsname}/{snap.snapname}")
            return

        if status is None:
            # already gone? make sure it shows that way in the logs
            intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "delete", "complete")
            log.info(f"snap {snap.fsname}/{snap.snapname} sucessfully deleted")
            return
        else:
            locator = status['locator']

        try:
            # ask cluster to delete the snap
            deleted_snap = snap.cluster_obj.call_api(method="snapshot_delete",
                                                parms={"file_system": snap.fsname, "name": snap.snapname})
            log.info(f"snap {snap.fsname}/{snap.snapname} delete initiated")
            #log.debug(f"data returned = {deleted_snap}")
        except Exception as exc:
            log.error(f"error deleting snapshot {snap.snapname} from filesystem {snap.fsname}: {exc}")

        intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "delete", "in-progress")
        snaplog.info(f"Delete Initiated:{snap.fsname}:{snap.snapname}:{locator}")

        # delete may take some time, particularly if uploaded to obj and it's big
        time.sleep(1)  # give just a little time, just in case it's instant
        delete_complete = False
        sleeptime = 5.0
        loopcount = 0
        while not delete_complete:
            # if may happen quickly, so sleep at the end of the cycle
            try:
                this_snap = snapshot_status(snap)
            except Exception:
                # when the snap no longer exists, we get a None back, so this is an error
                #log.debug(f"snap delete raised exception")
                log.error(f"error listing snapshots: checking status")
                return

            # when the snap no longer exists, we get a None from snap_status()
            if this_snap is None:
                intent_log.put_record(snap.uuid, snap.fsname, snap.snapname, "delete", "complete")
                log.info(f"snap {snap.fsname}/{snap.snapname} sucessfully deleted")
                snaplog.info(f"Delete complete:{snap.fsname}:{snap.snapname}:{locator}")
                return
            #log.debug(f"this_snap is {this_snap}")
            # track how many times we're checking the status
            progress = int(this_snap['objectProgress'][:-1])  # progress is something like "33%"
            loopcount += 1
            log.info(
                f"delete of {snap.fsname}/{snap.snapname} in progress: {this_snap['objectProgress']} complete")

            # reduce log spam - seems to hang under 50% for a while
            sleeptime = sleeptimer(loopcount, progress)

            time.sleep(sleeptime)  # give it some time to delete, check in every 5s


    """
    # not using this yet (maybe never)... accesspoint_name is a little issue
    def create_snap(snap):
        log.debug(f"creating snap {snap.fsname}/{snap.snapname}")

        log.debug(f"snap {snap.snapname} to be created on fs {snap.fsname}")
        try:
            created_snap = snap.cluster_obj.call_api(method="snapshot_create", parms={
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
            # uploadq.get() returns a QueueOperation object
            # log.debug(f"Getting from queue")
            snap = uploadq.get(block=True, timeout=1)  # block for 1s
        except queue.Empty:
            # log.debug(f"Queue get timed out; nothing in queue.")
            if main_thread.is_alive():
                # log.debug(f"Main thread is alive")
                continue
            else:
                log.debug(f"Main thread is dead, exiting uploader thread")
                # main thread died - exit so the program exits; we can't live without the main thread
                return

        log.debug(f"Queue entry received {snap.fsname}, {snap.snapname}, {snap.operation}")

        if snap.fsname == "WEKA_TERMINATE_THREAD" and snap.snapname == "WEKA_TERMINATE_THREAD":
            log.info(f"background_processor: terminating thread")
            return

        time.sleep(5)   # slow down... make sure the snap is settled.

        if snap.operation == "upload":
            upload_snap(snap)   # handles it's own errors
        elif snap.operation == "delete":
            delete_snap(snap)
        #elif snap.operation == "create":
            #create_snap(snap)


# module init
# upload queue for queuing object uploads
uploadq = queue.Queue()
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

    for uuid, fs, operation, snap in intent_log._incomplete_records():
        print(f"uuid={uuid}, fs={fs}, operation'{operation}, snap={snap}")
"""

    logging.debug(f"first test")
    UploadSnapshot(uploadq, "fs1", "snap1") # should be cluster instead of uploadq

    logging.debug(f"second test")
    UploadSnapshot(uploadq, "fs2", "snap2")


    logging.debug(f"third test")
    UploadSnapshot(uploadq, "fs3", "snap3")

    logging.debug(f"fourth test")
    UploadSnapshot(uploadq, "fs4", "snap4")


    logging.debug(f"fifth test")
    UploadSnapshot(uploadq, "fs5", "snap5")

    logging.debug(f"sixth test")
    UploadSnapshot(uploadq, "fs6", "snap6")

    logging.debug(f"terminating")
    UploadSnapshot(uploadq, "WEKA_TERMINATE_THREAD", "WEKA_TERMINATE_THREAD")

    time.sleep(15)

    upload_thread.join()
"""
