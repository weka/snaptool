#!/usr/bin/env python3

# upload.py - manage snapshot uploads
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
    def put_record(self, uuid, fsname, snapname, status):
        with self._lock:
            with open(self.filename, "a") as fd:
                fd.write(f"{uuid}:{fsname}:{snapname}:{status}\n")

    # replay the log on a cluster
    def replay(self, cluster):
        log.critical(f"Replaying upload intent log")
        for uuid, fsname, snapname in self._incomplete_records():
            log.info(f"re-scheduling {fsname}/{snapname}")
            UploadSnapshot(cluster, fsname, snapname, uuid=uuid)

    # yield back all records - returns uuid, fsname, snapname, status
    def _records(self):
        with self._lock:
            for filename in [self.filename + '.1', self.filename]:
                try:
                    with open(filename, "r") as fd:
                        for record in fd:
                            temp = record.split(':')
                            yield temp[0], temp[1], temp[2], temp[3][:-1]
                except FileNotFoundError:
                    log.info(f"Log file {filename} not found")
                    continue

    # un-completed records - an iterable
    def _incomplete_records(self):
        snaps = {}
        # distill the records to just ones that need to be re-uploaded
        for uuid, fsname, snapname, status in intent_log._records():
            if uuid not in snaps:
                snaps[uuid] = {}
                snaps[uuid]['fsname'] = fsname
                snaps[uuid]['snapname'] = snapname
                snaps[uuid]['status'] = status
            else:
                # log.debug(f"status is '{status}'")
                if status == "complete":
                    log.debug(f"Deleting complete snap {uuid}")
                    del snaps[uuid]  # remove ones that completed so we don't need to look through them
                else:
                    log.debug(f"Updating status of snap {uuid} to {status}")
                    snaps[uuid]['status'] = status  # update status

        log.debug(f"snaps = {snaps}")  # this should be a very short list - far less than 100; likely under 5

        sorted_snaps = {"queued": {}, "uploading": {}, "error": {}}

        for uuid, snapshot in snaps.items():  # sort so we can see uploading and error first
            log.debug(f"uuid={uuid}, snapshot={snapshot}")
            sorted_snaps[snapshot['status']][uuid] = snapshot

        log.debug(f"sorted_snaps = {sorted_snaps}")
        log.debug(
            f"There are {len(sorted_snaps['error'])} error snaps, {len(sorted_snaps['uploading'])} uploading" +
            "snaps, and {len(sorted_snaps['queued'])} queued snaps in the intent log")

        # process in order of status
        for status in ["uploading", "error",
                       "queued"]:  # not sure about error ones... do we re-queue?  Should only be 1 uploading too
            for uuid, snapshot in sorted_snaps[status].items():
                # these should be re-queued because they didn't finish
                log.debug(f"re-queueing snapshot = {snapshot}, status={status}")
                yield uuid, snapshot['fsname'], snapshot['snapname']


def unique_id(alphabet='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    number = uuid.uuid4().int
    result = ''
    while number != 0:
        number, i = divmod(number, len(alphabet))
        result = alphabet[i] + result
    return result


class UploadSnapshot:
    def __init__(self, cluster, fsname, snapname, uuid=None):
        global uploadq
        global intent_log
        self.fsname = fsname
        self.snapname = snapname
        self.cluster_obj = cluster

        if uuid is None:
            self.uuid = unique_id()
        else:
            self.uuid = uuid
        if fsname != "WEKA_TERMINATE_THREAD" and snapname != "WEKA_TERMINATE_THREAD":
            intent_log.put_record(self.uuid, fsname, snapname, "queued")
        # queue the request
        uploadq.put(self)


# starts the background uploader
# def start_uploader():
#    upload_thread = threading.Thread(target=background_uploader)
#    upload_thread.start()
#    log.info(f"upload_thread = {upload_thread}")


# uploads snapshots in the background - runs in a thread - starts before replaying log
def background_uploader():
    global uploadq
    global intent_log
    log.info("background_uploader starting...")

    main_thread = threading.main_thread()

    while True:
        # take item off queue
        try:
            # don't block forever so we can keep an eye on the main thread
            # log.debug(f"Getting from queue")
            snap = uploadq.get(block=True, timeout=10)  # block for 10s
        except queue.Empty:
            # log.debug(f"Queue get timed out; nothing in queue.")
            if main_thread.is_alive():
                # log.debug(f"Main thread is alive")
                continue
            else:
                log.debug(f"Main thread is dead, exiting uploader thread")
                # main thread died - exit so the program exits; we can't live without the main thread
                return

        log.debug(f"Queue entry received {snap}")
        fsname = snap.fsname
        snapname = snap.snapname
        cluster_obj = snap.cluster_obj

        if fsname == "WEKA_TERMINATE_THREAD" and snapname == "WEKA_TERMINATE_THREAD":
            log.info(f"background_uploader: terminating thread")
            return

        # get snap info via api
        try:
            snap_stat = cluster_obj.call_api(method="snapshots_list",
                                             parms={'file_system': snap.fsname, 'name': snap.snapname})
        except Exception as exc:
            log.error(f"error listing snapshots: checking status: {exc}")
            continue

        if len(snap_stat) == 0:
            # hmm... this one doesn't exist on the cluster?
            log.error(f"{snap.fsname}/{snap.snapname} doesn't exist! Did creation fail?  Logging as complete...")
            intent_log.put_record(snap.uuid, fsname, snapname, "complete")
            continue
        else:
            log.debug(f"snap_stat is {snap_stat}")
            this_snap = snap_stat[0]

        if this_snap["stowStatus"] == "NONE":
            # Hasn't been uploaded yet; Try to upload the snap via API
            try:
                snapshots = cluster_obj.call_api(method="snapshot_upload",
                                                 parms={'file_system': snap.fsname, 'snapshot': snap.snapname})
            except Exception as exc:
                log.error(f"error uploading snapshot {snap.fsname}/{snap.snapname}: {exc}")
                intent_log.put_record(snap.uuid, fsname, snapname, "error")
                continue  # skip the rest for this one

            # log that it's been told to upload
            # ***vince - check the return to make sure it's been told to upload
            log.info(f"uploading snapshot {snap.fsname}/{snap.snapname}")
            intent_log.put_record(snap.uuid, fsname, snapname, "uploading")

        elif this_snap["stowStatus"] == "SYNCHRONIZED":
            # we should only ever get here when replaying the log and this one was already in progress
            log.error(f"upload of {snap.fsname}/{snap.snapname} was already complete. Logging it as such")
            intent_log.put_record(snap.uuid, fsname, snapname, "complete")

        # otherwise, it should be uploading, so we fall through and monitor it

        # monitor progress - we have to wait for this one to complete before uploading another
        upload_complete = False
        while not upload_complete:
            time.sleep(10)  # give it some time to upload, check in every 10s
            # get snap info via api
            try:
                snap_stat = cluster_obj.call_api(method="snapshots_list",
                                                 parms={'file_system': snap.fsname, 'name': snap.snapname})
            except Exception as exc:
                log.error(f"error listing snapshots: checking status: {exc}")
            if len(snap_stat) > 0:
                this_snap = snap_stat[0]
                if this_snap["stowStatus"] == "UPLOADING":
                    log.debug(
                        f"upload of {snap.fsname}/{snap.snapname} in progress: {this_snap['objectProgress']} complete")
                    continue
                elif this_snap["stowStatus"] == "SYNCHRONIZED":
                    log.info(f"upload of {snap.fsname}/{snap.snapname} complete.")
                    intent_log.put_record(snap.uuid, fsname, snapname, "complete")
                    upload_complete = True
                    continue
                else:
                    log.error(
                        f"upload status of {snap.fsname}/{snap.snapname} is {this_snap['stowStatus']}/" +
                        "{this_snap['objectProgress']}?")
                    continue
            else:
                log.error(f"no snap status for {snap.fsname}/{snap.snapname}?")
                continue


# module init
# upload queue for queuing object uploads
uploadq = queue.Queue()
# intent log
intent_log = IntentLog("upload_intent.log")

# start the upload thread
upload_thread = threading.Thread(target=background_uploader)
upload_thread.start()
log.info(f"upload_thread = {upload_thread}")

if __name__ == "__main__":

    time.sleep(2)

    intent_log.put_record('uuid1', "fs1", 'snap1', "queued")
    intent_log.put_record('uuid2', "fs1", 'snap2', "queued")
    intent_log.put_record('uuid3', "fs1", 'snap3', "queued")
    intent_log.put_record('uuid4', "fs1", 'snap4', "queued")

    intent_log.put_record('uuid1', "fs1", 'snap1', "uploading")
    intent_log.put_record('uuid2', "fs1", 'snap2', "uploading")

    intent_log.put_record('uuid1', "fs1", 'snap1', "complete")

    for uuid, fs, snap in intent_log._incomplete_records():
        print(f"uuid={uuid}, fs={fs}, snap={snap}")
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
