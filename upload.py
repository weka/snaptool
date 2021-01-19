#!/usr/bin/env python3

# upload.py - manage snapshot uploads
# Vince Fleming
# vince@weka.io
#

# system imports
import logging
import logging.handlers
from logging import debug, info, warning, error, critical, getLogger
import time
import uuid
import threading
#import datetime
#from urllib3 import add_stderr_logger
import queue

#from wekacluster import WekaCluster
#import signals
#from snapshots import SnapSchedule, MonthlySchedule, WeeklySchedule, DailySchedule, HourlySchedule

log = getLogger(__name__)

# module init
upload_intent_log = logging.getLogger("upload_intent_log")

upload_intent_handler = logging.handlers.RotatingFileHandler("upload_intent.log",maxBytes=1024*1024, backupCount=1)
upload_intent_handler.setFormatter(logging.Formatter("%(message)s"))

upload_intent_log.addHandler(upload_intent_handler)

# upload queue for queuing object uploads
uploadq = queue.Queue()


def unique_id(alphabet='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    number = uuid.uuid4().int
    result = ''
    while number != 0:
        number, i = divmod(number, len(alphabet))
        result = alphabet[i] + result
    return result

class UploadSnapshot():
    def __init__(self, cluster, fsname, snapname):
        global uploadq
        intent_log = logging.getLogger("upload_intent_log")
        self.fsname = fsname
        self.snapname = snapname
        self.cluster_obj = cluster
       
        self.uuid = unique_id()
        if fsname != "WEKA_TERMINATE_THREAD" and snapname != "WEKA_TERMINATE_THREAD":
            intent_log.info(f"{self.uuid}:{fsname}:{snapname}:queued")
        # queue the request
        uploadq.put(self)

# uploads snapshots in the background - runs in a thread
def background_uploader():
    global uploadq
    log.info("background_uploader starting...")
    intent_log = logging.getLogger("upload_intent_log")

    while True:
        # take item off queue
        snap = uploadq.get(block=True, timeout=None)    # block forever until something comes in

        fsname = snap.fsname
        snapname = snap.snapname
        cluster_obj = snap.cluster_obj

        if fsname == "WEKA_TERMINATE_THREAD" and snapname == "WEKA_TERMINATE_THREAD":
            log.info(f"background_uploader: terminating thread")
            return

        # Try to upload the snap via API
        try:
            snapshots = cluster_obj.call_api(method="snapshot_upload",parms={'file_system': snap.fsname, 'snapshot': snap.snapname})
        except Exception as exc:
            log.error(f"error uploading snapshot {snap.fsname}/{snap.snapname}: {exc}")
            intent_log.info(f"{snap.uuid}:{fsname}:{snapname}:error")
            continue    # skip the rest for this one
        
        intent_log.info(f"{snap.uuid}:{fsname}:{snapname}:uploading")

        # monitor progress - we have to wait for this one to complete before uploading another
        upload_complete = False
        while not upload_complete:
            time.sleep(10)  # give it some time to upload, check in every 10s
            # get snap info via api
            try:
                snap_stat = cluster_obj.call_api(method="snapshots_list",parms={'file_system': snap.fsname, 'name': snap.snapname})
            except Exception as exc:
                log.error(f"error listing snapshots: checking status: {exc}")
            if len(snap_stat) > 0:
                this_snap = snap_stat[0]
                if this_snap["stowStatus"] == "UPLOADING":
                    log.debug(f"upload of {snap.fsname}/{snap.snapname} in progress: {this_snap['objectProgress']} complete")
                    continue
                elif this_snap["stowStatus"] == "SYNCHRONIZED":
                    log.info(f"upload of {snap.fsname}/{snap.snapname} complete.")
                    intent_log.info(f"{snap.uuid}:{fsname}:{snapname}:complete")
                    upload_complete = True
                    continue
                else:
                    log.error(f"upload status of {snap.fsname}/{snap.snapname} is {this_snap['stowStatus']}/{this_snap['objectProgress']}?")
                    continue
            else:
                log.error(f"no snap status for {snap.fsname}/{snap.snapname}?")
                continue

def replay_upload_intent_log():
    snaps = {}
    with open("upload_intent.log") as f:
        for logentry in f:
            fields = logentry.split(':')
            uuid = fields[0]
            fsname = fields[1]
            snapname = fields[2]
            status = fields[3]
            if uuid not in snaps:
                snaps[uuid] = {}
                snaps[uuid][fsname] = fsname
                snaps[uuid][snapname] = snapname
                snaps[uuid][status] = status
            else:
                if status == "complete":
                    del snaps[uuid]     # remove ones that completed so we don't need to look through them
                else:
                    snaps[uuid][status] = status    # update status

    # force a rollover of the log so we start clean
    upload_intent_handler.doRollover()

    for snapshot in snaps:      # these should be re-queued because they didn't finish
        if snapshot[status] == "queued":    # can be "queued", "uploading", "error", and "complete"
            yield snapshot[fsname], snapshot[snapname]


# start the background uploader
upload_thread = threading.Thread(target=background_uploader)
upload_thread.start()
log.info(f"upload_thread = {upload_thread}")

if __name__ == "__main__":

    time.sleep(2)


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
